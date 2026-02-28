use memchr::{memchr2, memchr2_iter, memmem};
use memmap2::Mmap;
use rayon::prelude::*;
use std::ffi::CStr;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::raw::c_char;
use std::ptr;

// classic piece table implementation.
// Original = points to the readonly memory mapped file.
// Memory = points to heap allocated edits.
#[derive(Clone)]
enum Piece {
    Original { start_line: usize, line_count: usize },
    Memory { start_idx: usize, line_count: usize },
}

impl Piece {
    fn line_count(&self) -> usize {
        match self {
            Piece::Original { line_count, .. } => *line_count,
            Piece::Memory { line_count, .. } => *line_count,
        }
    }
}

struct ChunkMeta {
    byte_offset: usize,
    start_line: usize,
}

pub struct LogEngine {
    mmap: Mmap,
    chunks: Vec<ChunkMeta>,
    original_total_lines: usize,
    pieces: Vec<Piece>,
    memory_buffer: Vec<String>,
    last_block: String, // persistent buffer to hand out safe pointers to C
}

impl LogEngine {
    fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        #[cfg(unix)]
        unsafe {
            // give the OS a heads up. sequential for parsing now, random for actual usage later.
            libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
            libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_RANDOM,
            );
        }

        // blast through the file in 1MB chunks to count lines.
        let chunk_size = 1024 * 1024;
        let line_counts: Vec<usize> = mmap
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut count = 0;
                let mut iter = memchr2_iter(b'\n', b'\r', chunk).peekable();
                while let Some(pos) = iter.next() {
                    count += 1;
                    // the \r\n check here is slightly cursed but prevents overcounting windows line endings.
                    if chunk[pos] == b'\r' {
                        if let Some(&next_pos) = iter.peek() {
                            if next_pos == pos + 1 && chunk[next_pos] == b'\n' {
                                iter.next();
                            }
                        }
                    }
                }
                count
            })
            .collect();

        let mut chunks = Vec::with_capacity(line_counts.len());
        let mut current_line = 0;

        for (i, &count) in line_counts.iter().enumerate() {
            let byte_offset = i * chunk_size;
            // what happens if \r is at the end of chunk N and \n is at the start of chunk N+1?
            // this. this happens. adjust the line count so we don't desync.
            if i > 0 && mmap[byte_offset - 1] == b'\r' && mmap.get(byte_offset) == Some(&b'\n') {
                current_line -= 1;
            }
            chunks.push(ChunkMeta {
                byte_offset,
                start_line: current_line,
            });
            current_line += count;
        }

        let mut original_total_lines = current_line;
        if !mmap.is_empty() {
            // handle files without a trailing newline
            let last_byte = mmap.last().copied();
            if last_byte != Some(b'\n') && last_byte != Some(b'\r') {
                original_total_lines += 1;
            }
            if original_total_lines == 0 {
                original_total_lines = 1;
            }
        }

        let pieces = vec![Piece::Original {
            start_line: 0,
            line_count: original_total_lines,
        }];

        Ok(LogEngine {
            mmap,
            chunks,
            original_total_lines,
            pieces,
            memory_buffer: Vec::new(),
            last_block: String::new(),
        })
    }

    fn line_to_byte_offset(&self, line: usize) -> usize {
        if line >= self.original_total_lines {
            return self.mmap.len();
        }
        
        // find the closest chunk behind our target line
        let chunk_idx = match self.chunks.binary_search_by_key(&line, |c| c.start_line) {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        };
        
        let chunk = &self.chunks[chunk_idx];
        let mut offset = chunk.byte_offset;
        let mut skip = line - chunk.start_line;
        
        // walk the rest of the bytes manually until we hit the exact line
        while skip > 0 && offset < self.mmap.len() {
            let slice = &self.mmap[offset..];
            if let Some(pos) = memchr2(b'\n', b'\r', slice) {
                offset += pos + 1;
                if slice[pos] == b'\r' && offset < self.mmap.len() && self.mmap[offset] == b'\n' {
                    offset += 1; // skip the \n of a \r\n pair
                }
                skip -= 1;
            } else {
                offset = self.mmap.len();
                break;
            }
        }
        offset
    }

    fn get_original_bytes(&self, start_line: usize, line_count: usize) -> &[u8] {
        if line_count == 0 {
            return &[];
        }
        let start = self.line_to_byte_offset(start_line);
        let end = self.line_to_byte_offset(start_line + line_count);
        &self.mmap[start..end]
    }

    fn total_lines(&self) -> usize {
        self.pieces.iter().map(|p| p.line_count()).sum()
    }

    // returns (piece_index, line_offset_inside_piece)
    fn find_piece_idx(&self, logical_line: usize) -> (usize, usize) {
        let mut current = 0;
        for (i, piece) in self.pieces.iter().enumerate() {
            let count = piece.line_count();
            if logical_line < current + count {
                return (i, logical_line - current);
            }
            current += count;
        }
        (self.pieces.len(), 0)
    }

    fn split_piece_at(&mut self, piece_idx: usize, offset: usize) {
        if offset == 0 || piece_idx >= self.pieces.len() {
            return;
        }
        let piece = self.pieces[piece_idx].clone();
        if offset >= piece.line_count() {
            return;
        }

        match piece {
            Piece::Original { start_line, line_count } => {
                self.pieces[piece_idx] = Piece::Original { start_line, line_count: offset };
                self.pieces.insert(piece_idx + 1, Piece::Original {
                    start_line: start_line + offset,
                    line_count: line_count - offset,
                });
            }
            Piece::Memory { start_idx, line_count } => {
                self.pieces[piece_idx] = Piece::Memory { start_idx, line_count: offset };
                self.pieces.insert(piece_idx + 1, Piece::Memory {
                    start_idx: start_idx + offset,
                    line_count: line_count - offset,
                });
            }
        }
    }

    fn apply_edit(&mut self, start_line: usize, num_deleted: usize, new_text: &str) {
        let (mut piece_idx, offset) = self.find_piece_idx(start_line);

        if piece_idx < self.pieces.len() {
            self.split_piece_at(piece_idx, offset);
            if offset > 0 {
                piece_idx += 1;
            }
        }

        let mut remaining_delete = num_deleted;
        
        // nuke pieces fully contained in the deletion range
        while remaining_delete > 0 && piece_idx < self.pieces.len() {
            let count = self.pieces[piece_idx].line_count();
            if count <= remaining_delete {
                self.pieces.remove(piece_idx);
                remaining_delete -= count;
            } else {
                // partial overlap, split and drop the front
                self.split_piece_at(piece_idx, remaining_delete);
                self.pieces.remove(piece_idx);
                remaining_delete = 0;
            }
        }

        if !new_text.is_empty() {
            let mut lines: Vec<String> = new_text.split('\n').map(|s| s.to_string()).collect();
            // drop the trailing empty string from split if it exists
            if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
                lines.pop();
            }
            if !lines.is_empty() {
                let start_idx = self.memory_buffer.len();
                let line_count = lines.len();
                self.memory_buffer.extend(lines);
                self.pieces.insert(piece_idx, Piece::Memory { start_idx, line_count });
            }
        }
    }

    fn get_block(&mut self, start_line: usize, num_lines: usize) -> *const u8 {
        self.last_block.clear();
        if num_lines == 0 || start_line >= self.total_lines() {
            return ptr::null();
        }

        let (mut piece_idx, mut offset) = self.find_piece_idx(start_line);
        let mut collected = 0;

        // stitch together pieces until we satisfy the requested line count
        while collected < num_lines && piece_idx < self.pieces.len() {
            let piece = &self.pieces[piece_idx];
            let count = piece.line_count() - offset;
            let take = count.min(num_lines - collected);

            match piece {
                Piece::Original { start_line: p_start, .. } => {
                    let start_byte = self.line_to_byte_offset(p_start + offset);
                    let end_byte = self.line_to_byte_offset(p_start + offset + take);
                    
                    let bytes = &self.mmap[start_byte..end_byte];
                    
                    // logs are dirty. replace garbage bytes with  instead of failing silently.
                    let s = String::from_utf8_lossy(bytes);
                    self.last_block.push_str(&s);
                    if !self.last_block.ends_with('\n') && !self.last_block.is_empty() {
                        self.last_block.push('\n');
                    }
                }
                Piece::Memory { start_idx, .. } => {
                    for i in 0..take {
                        self.last_block.push_str(&self.memory_buffer[start_idx + offset + i]);
                        self.last_block.push('\n');
                    }
                }
            }
            collected += take;
            offset = 0;
            piece_idx += 1;
        }

        // C side expects a pointer. this gets overwritten next call, DO NOT keep it around.
        self.last_block.as_ptr()
    }

    fn save(&self, path: &str) -> bool {
        let temp_path = format!("{}.tmp", path);
        let file = match OpenOptions::new().write(true).create(true).truncate(true).open(&temp_path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mut writer = BufWriter::new(file);

        for piece in &self.pieces {
            match piece {
                Piece::Original { start_line, line_count } => {
                    let bytes = self.get_original_bytes(*start_line, *line_count);
                    if writer.write_all(bytes).is_err() {
                        return false;
                    }
                    if !bytes.ends_with(b"\n") && !bytes.is_empty() {
                        if writer.write_all(b"\n").is_err() {
                            return false;
                        }
                    }
                }
                Piece::Memory { start_idx, line_count } => {
                    for i in 0..*line_count {
                        if writer.write_all(self.memory_buffer[start_idx + i].as_bytes()).is_err() {
                            return false;
                        }
                        if writer.write_all(b"\n").is_err() {
                            return false;
                        }
                    }
                }
            }
        }

        if writer.flush().is_err() {
            return false;
        }
        // atomic swap
        std::fs::rename(&temp_path, path).is_ok()
    }
}

// --- C ABI Boundary ---
// Trusting the caller from here on out. standard unsafe boilerplate.

#[no_mangle]
pub extern "C" fn log_engine_new(path: *const c_char) -> *mut LogEngine {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_str = unsafe { CStr::from_ptr(path) };
    // paths can be cursed too on some OSes.
    let path_str = c_str.to_string_lossy();
    if let Ok(engine) = LogEngine::new(path_str.as_ref()) {
        return Box::into_raw(Box::new(engine));
    }
    ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn log_engine_total_lines(engine: *const LogEngine) -> usize {
    let engine = unsafe {
        if engine.is_null() {
            return 0;
        }
        &*engine
    };
    engine.total_lines()
}

#[no_mangle]
pub extern "C" fn log_engine_get_block(
    engine: *mut LogEngine,
    start_line: usize,
    num_lines: usize,
    out_len: *mut usize,
) -> *const u8 {
    let engine = unsafe {
        if engine.is_null() {
            return ptr::null();
        }
        &mut *engine
    };
    let ptr = engine.get_block(start_line, num_lines);
    if !out_len.is_null() {
        unsafe { *out_len = engine.last_block.len() };
    }
    ptr
}

#[no_mangle]
pub extern "C" fn log_engine_apply_edit(
    engine: *mut LogEngine,
    start_line: usize,
    num_deleted: usize,
    new_text: *const c_char,
) {
    let engine = unsafe {
        if engine.is_null() {
            return;
        }
        &mut *engine
    };
    // nvim might send weird stuff, salvage what we can.
    let text = if new_text.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(new_text) }.to_string_lossy().into_owned()
    };
    engine.apply_edit(start_line, num_deleted, &text);
}

#[no_mangle]
pub extern "C" fn log_engine_save(engine: *const LogEngine, path: *const c_char) -> bool {
    let engine = unsafe {
        if engine.is_null() {
            return false;
        }
        &*engine
    };
    if path.is_null() {
        return false;
    }
    // paths can be cursed too.
    let path_str = unsafe { CStr::from_ptr(path) }.to_string_lossy();
    return engine.save(path_str.as_ref());
}

#[no_mangle]
pub extern "C" fn log_engine_search(
    engine: *const LogEngine,
    query: *const c_char,
    start_line: usize,
) -> isize {
    let engine = unsafe {
        if engine.is_null() {
            return -1;
        }
        &*engine
    };
    if query.is_null() {
        return -1;
    }
    let query_bytes = match unsafe { CStr::from_ptr(query) }.to_bytes_with_nul().split_last() {
        Some((&0, bytes)) => bytes,
        _ => return -1,
    };
    if query_bytes.is_empty() {
        return -1;
    }

    let (mut piece_idx, mut offset) = engine.find_piece_idx(start_line);
    let mut current_logical = start_line;

    while piece_idx < engine.pieces.len() {
        let piece = &engine.pieces[piece_idx];
        match piece {
            Piece::Original { start_line: p_start, line_count } => {
                let bytes = engine.get_original_bytes(p_start + offset, line_count - offset);
                if let Some(pos) = memmem::find(bytes, query_bytes) {
                    
                    // found the byte offset, now manually count newlines up to this point
                    // to resolve the actual logical line number. slow but accurate.
                    let slice_to_match = &bytes[..pos];
                    let mut lines = 0;
                    let mut iter = memchr2_iter(b'\n', b'\r', slice_to_match).peekable();
                    while let Some(p) = iter.next() {
                        lines += 1;
                        if slice_to_match[p] == b'\r' {
                            if let Some(&np) = iter.peek() {
                                if np == p + 1 && slice_to_match[np] == b'\n' {
                                    iter.next();
                                }
                            }
                        }
                    }
                    return (current_logical + lines) as isize;
                }
            }
            Piece::Memory { start_idx, line_count } => {
                // query might be cursed too.
                let q_str = String::from_utf8_lossy(query_bytes);
                for i in offset..*line_count {
                    if engine.memory_buffer[start_idx + i].contains(q_str.as_ref()) {
                        return (current_logical + i - offset) as isize;
                    }
                }
            }
        }
        current_logical += piece.line_count() - offset;
        offset = 0;
        piece_idx += 1;
    }
    -1
}

#[no_mangle]
pub extern "C" fn log_engine_search_backward(
    engine: *const LogEngine,
    query: *const c_char,
    start_line: usize,
) -> isize {
    let engine = unsafe {
        if engine.is_null() {
            return -1;
        }
        &*engine
    };
    if query.is_null() {
        return -1;
    }
    let query_bytes = match unsafe { CStr::from_ptr(query) }.to_bytes_with_nul().split_last() {
        Some((&0, bytes)) => bytes,
        _ => return -1,
    };
    if query_bytes.is_empty() {
        return -1;
    }

    let (mut piece_idx, mut offset) = engine.find_piece_idx(start_line);
    if piece_idx >= engine.pieces.len() {
        piece_idx = engine.pieces.len().saturating_sub(1);
        offset = engine.pieces[piece_idx].line_count().saturating_sub(1);
    }

    let mut current_logical = start_line;

    // walking backwards through pieces. same logic as forward search but reversed.
    loop {
        let piece = &engine.pieces[piece_idx];
        match piece {
            Piece::Original { start_line: p_start, .. } => {
                let bytes = engine.get_original_bytes(*p_start, offset + 1);
                if let Some(pos) = memmem::rfind(bytes, query_bytes) {
                    let slice_to_match = &bytes[..pos];
                    let mut lines = 0;
                    let mut iter = memchr2_iter(b'\n', b'\r', slice_to_match).peekable();
                    while let Some(p) = iter.next() {
                        lines += 1;
                        if slice_to_match[p] == b'\r' {
                            if let Some(&np) = iter.peek() {
                                if np == p + 1 && slice_to_match[np] == b'\n' {
                                    iter.next();
                                }
                            }
                        }
                    }
                    return (current_logical - offset + lines) as isize;
                }
            }
            Piece::Memory { start_idx, .. } => {
                // query might be cursed too.
                let q_str = String::from_utf8_lossy(query_bytes);
                for i in (0..=offset).rev() {
                    if engine.memory_buffer[start_idx + i].contains(q_str.as_ref()) {
                        return (current_logical - offset + i) as isize;
                    }
                }
            }
        }

        if piece_idx == 0 {
            break;
        }
        current_logical = current_logical.saturating_sub(offset + 1);
        piece_idx -= 1;
        offset = engine.pieces[piece_idx].line_count().saturating_sub(1);
    }
    -1
}

#[no_mangle]
pub extern "C" fn log_engine_free(engine: *mut LogEngine) {
    if !engine.is_null() {
        unsafe {
            // reclaim ownership and let Rust's drop cleanup the memory
            let _ = Box::from_raw(engine);
        }
    }
}
