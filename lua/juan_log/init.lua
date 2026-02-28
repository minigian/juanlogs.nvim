local ffi = require("ffi")
local M = {}

local config = {
    threshold_size = 1024 * 1024 * 100, -- 100MB trigger
    mode = "dynamic",
    dynamic_chunk_size = 10000,
    dynamic_margin = 2000, -- reload when we get this close to the edge
    patterns = { "*" },
    enable_custom_statuscol = true,
    syntax = false
}

-- keep this in sync with the rust struct/externs or segfaults will happen.
ffi.cdef [[
    typedef struct LogEngine LogEngine;
    LogEngine* log_engine_new(const char* path);
    size_t log_engine_total_lines(LogEngine* engine);
    const char* log_engine_get_block(LogEngine* engine, size_t start_line, size_t num_lines, size_t* out_len);
    void log_engine_apply_edit(LogEngine* engine, size_t start_line, size_t num_deleted, const char* new_text);
    bool log_engine_save(LogEngine* engine, const char* path);
    long log_engine_search(LogEngine* engine, const char* query, size_t start_line);
    long log_engine_search_backward(LogEngine* engine, const char* query, size_t start_line);
    void log_engine_free(LogEngine* engine);
]]

local function get_lib_path()
    local sysname = vim.loop.os_uname().sysname
    local ext = sysname == "Windows_NT" and "dll" or (sysname == "Darwin" and "dylib" or "so")
    local lib_name = "libjuanlog." .. ext

    -- check local dev path first, useful for debugging without reinstalling
    local local_dev_path = vim.fn.stdpath("config") .. "/lua/juan_log/bin/" .. lib_name
    if vim.loop.fs_stat(local_dev_path) then
        return local_dev_path
    end

    -- fallback to release path
    local str = debug.getinfo(1, "S").source:sub(2)
    local plugin_root = str:match("(.*[/\\])"):gsub("lua[/\\]juan_log[/\\]$", "")
    return plugin_root .. "target/release/" .. lib_name
end

local so_path = get_lib_path()
local ok, lib = pcall(ffi.load, so_path)

if not ok then
    vim.schedule(function()
        vim.notify("[JuanLog] Advertencia: No se encontró el binario de Rust.\nEl visor de logs gigantes está desactivado.", vim.log.levels.WARN)
    end)
    lib = nil
end

-- global state to map buffers to rust engines
_G.JuanLogStates = _G.JuanLogStates or {}

-- custom status column to fake absolute line numbers.
-- since the buffer only holds a small chunk, 'lnum' is wrong relative to the file.
_G._juan_log_statuscol = function()
    local winid = vim.g.statusline_winid or vim.api.nvim_get_current_win()
    local b = vim.api.nvim_win_get_buf(winid)
    local st = _G.JuanLogStates[b]
    
    if st and config.mode == "dynamic" then
        return string.format("%%=%d ", st.offset + vim.v.lnum)
    end
    return "%=%l "
end

local function fetch_lines(engine, start, count)
    local len_ptr = ffi.new("size_t[1]")
    -- this pointer is only valid until the next call to rust. copy immediately.
    local block_ptr = lib.log_engine_get_block(engine, start, count, len_ptr)
    
    if block_ptr == nil then return {} end
    
    local length = tonumber(len_ptr[0])
    if length == 0 then return {} end

    local raw_text = ffi.string(block_ptr, length)
    
    -- clean up trailing newlines from the block fetch
    if raw_text:sub(-1) == "\n" then raw_text = raw_text:sub(1, -2) end
    if raw_text:sub(-1) == "\r" then raw_text = raw_text:sub(1, -2) end
    
    return vim.split(raw_text, "\n", { plain = true })
end

local function load_all_lines(bufnr, engine, total_lines)
    local chunk_size = 50000 
    local loaded = 0
    
    -- disable undo history or nvim RAM usage will skyrocket
    vim.api.nvim_buf_set_option(bufnr, 'undolevels', -1)
    
    while loaded < total_lines do
        local to_fetch = math.min(chunk_size, total_lines - loaded)
        local lines = fetch_lines(engine, loaded, to_fetch)
        
        if #lines > 0 then
            vim.api.nvim_buf_set_lines(bufnr, -1, -1, false, lines)
        end
        
        loaded = loaded + to_fetch
        
        -- force a redraw every few chunks so the UI doesn't freeze completely
        if loaded % (chunk_size * 5) == 0 then
            vim.cmd("redraw")
        end
    end
    
    vim.api.nvim_buf_set_option(bufnr, 'modified', false)
end

-- "teleport" the visible window to a new location in the huge file
local function jump_to_line(bufnr, state, found_line)
    local half_chunk = math.floor(config.dynamic_chunk_size / 2)
    local new_offset = math.max(0, found_line - half_chunk)

    if new_offset + config.dynamic_chunk_size > state.total then
        new_offset = math.max(0, state.total - config.dynamic_chunk_size)
    end

    state.updating = true
    local was_modified = vim.api.nvim_buf_get_option(bufnr, 'modified')
    local new_lines = fetch_lines(state.engine, new_offset, config.dynamic_chunk_size)
    
    -- replace the entire buffer content
    vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, new_lines)

    local new_row = (found_line - new_offset) + 1
    new_row = math.max(1, math.min(new_row, #new_lines))
    
    vim.api.nvim_win_set_cursor(0, {new_row, 0})
    
    state.offset = new_offset
    vim.api.nvim_buf_set_option(bufnr, 'modified', was_modified)
    state.updating = false
    
    vim.cmd("normal! zz")
end

local function setup_dynamic_window(bufnr, engine, total_lines, filepath)
    local state = {
        offset = 0,
        total = total_lines,
        bufnr = bufnr,
        engine = engine,
        updating = false, -- semaphore to prevent recursion loops
        last_query = nil,
        timer = vim.loop.new_timer()
    }
    _G.JuanLogStates[bufnr] = state

    state.updating = true
    local initial_lines = fetch_lines(engine, 0, config.dynamic_chunk_size)
    vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, initial_lines)
    vim.api.nvim_buf_set_option(bufnr, 'modified', false)
    state.updating = false

    local winid = vim.fn.bufwinid(bufnr)
    if winid ~= -1 and config.enable_custom_statuscol then
        vim.wo[winid].statuscolumn = "%!v:lua._juan_log_statuscol()"
        vim.wo[winid].number = true
    end

    -- listen for edits and send them to the rust piece table
    vim.api.nvim_buf_attach(bufnr, false, {
        on_lines = function(_, _, _, firstline, lastline, new_lastline)
            if state.updating then return end
            
            local start_line = state.offset + firstline
            local num_deleted = lastline - firstline
            
            local new_lines = vim.api.nvim_buf_get_lines(bufnr, firstline, new_lastline, false)
            local new_text = table.concat(new_lines, "\n")

            lib.log_engine_apply_edit(state.engine, start_line, num_deleted, new_text)
            state.total = tonumber(lib.log_engine_total_lines(state.engine))
        end
    })

    -- hijack save command
    vim.api.nvim_create_autocmd("BufWriteCmd", {
        buffer = bufnr,
        callback = function()
            local success = lib.log_engine_save(state.engine, filepath)
            if success then
                vim.api.nvim_buf_set_option(bufnr, 'modified', false)
            end
        end
    })

    -- infinite scrolling magic. 
    -- if cursor hits the margin, fetch next/prev chunk and shift everything.
    vim.api.nvim_create_autocmd({"CursorMoved", "CursorMovedI"}, {
        buffer = bufnr,
        callback = function()
            if state.updating then return end
            
            state.timer:stop()
            state.timer:start(15, 0, vim.schedule_wrap(function()
                if state.updating or not vim.api.nvim_buf_is_valid(bufnr) then return end

                local cursor = vim.api.nvim_win_get_cursor(0)
                local row = cursor[1]
                local buf_lines = vim.api.nvim_buf_line_count(bufnr)
                
                local shift_needed = false
                local new_offset = state.offset

                -- hit bottom margin?
                if row > (buf_lines - config.dynamic_margin) and (state.offset + buf_lines < state.total) then
                    local shift_amount = math.floor(config.dynamic_chunk_size / 2)
                    new_offset = state.offset + shift_amount
                    
                    if new_offset + config.dynamic_chunk_size > state.total then
                        new_offset = state.total - config.dynamic_chunk_size
                    end
                    shift_needed = true
                end

                -- hit top margin?
                if row < config.dynamic_margin and state.offset > 0 then
                    local shift_amount = math.floor(config.dynamic_chunk_size / 2)
                    new_offset = math.max(0, state.offset - shift_amount)
                    shift_needed = true
                end

                if shift_needed and new_offset ~= state.offset then
                    state.updating = true
                    local was_modified = vim.api.nvim_buf_get_option(bufnr, 'modified')
                    
                    local new_lines = fetch_lines(engine, new_offset, config.dynamic_chunk_size)
                    
                    -- swap buffer content seamlessly
                    vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, new_lines)
                    
                    -- adjust cursor relative to the new window
                    local new_row = (state.offset + row) - new_offset
                    new_row = math.max(1, math.min(new_row, #new_lines))
                    
                    vim.api.nvim_win_set_cursor(0, {new_row, cursor[2]})
                    
                    state.offset = new_offset
                    vim.api.nvim_buf_set_option(bufnr, 'modified', was_modified)
                    state.updating = false
                end
            end))
        end
    })
end

function M.attach_to_buffer(bufnr, filepath)
    if not lib then 
        return 
    end

    local engine = lib.log_engine_new(filepath)
    if engine == nil then 
        return 
    end

    local total_lines = tonumber(lib.log_engine_total_lines(engine))

    vim.api.nvim_buf_set_option(bufnr, 'buftype', 'acwrite')
    vim.api.nvim_buf_set_option(bufnr, 'swapfile', false)
    vim.api.nvim_buf_set_name(bufnr, filepath)
    
    -- turn off expensive stuff for huge files
    if not config.syntax then
        pcall(function() vim.opt_local.syntax = "off" end)
    else
        local ft = vim.filetype.match({ filename = filepath })
        if ft then
            vim.api.nvim_buf_set_option(bufnr, 'filetype', ft)
        end
    end
    pcall(function() vim.opt_local.spell = false end)

    if config.mode == "load_all" then
        load_all_lines(bufnr, engine, total_lines)
    else
        setup_dynamic_window(bufnr, engine, total_lines, filepath)
        
        -- standard / search won't work because lines aren't loaded.
        -- implementing custom search commands that query the engine.
        vim.api.nvim_buf_create_user_command(bufnr, "Logfind", function(opts)
            local state = _G.JuanLogStates[bufnr]
            if not state then return end

            local query = opts.args
            if query == "" then return end
            
            state.last_query = query

            local cursor = vim.api.nvim_win_get_cursor(0)
            local current_line_idx = state.offset + cursor[1] - 1 
            
            -- try to find the closest match (up or down)
            local start_down = current_line_idx + 1
            local found_down = tonumber(lib.log_engine_search(state.engine, query, start_down))

            local start_up = math.max(0, current_line_idx - 1)
            local found_up = -1
            
            if current_line_idx > 0 then
                found_up = tonumber(lib.log_engine_search_backward(state.engine, query, start_up))
            end

            local target_line = -1

            if found_down >= 0 and found_up >= 0 then
                local dist_down = found_down - current_line_idx
                local dist_up = current_line_idx - found_up
                if dist_up < dist_down then
                    target_line = found_up
                else
                    target_line = found_down
                end
            elseif found_down >= 0 then
                target_line = found_down
            elseif found_up >= 0 then
                target_line = found_up
            end

            if target_line >= 0 then
                jump_to_line(bufnr, state, target_line)
            end
        end, { nargs = 1 })

        -- remap 'n' and 'N'
        vim.keymap.set("n", "n", function()
            local state = _G.JuanLogStates[bufnr]
            if not state or not state.last_query then return end

            local cursor = vim.api.nvim_win_get_cursor(0)
            local start_line = state.offset + cursor[1]

            local found_line = tonumber(lib.log_engine_search(state.engine, state.last_query, start_line))

            if found_line >= 0 then
                jump_to_line(bufnr, state, found_line)
            end
        end, { buffer = bufnr, silent = true })

        vim.keymap.set("n", "N", function()
            local state = _G.JuanLogStates[bufnr]
            if not state or not state.last_query then return end

            local cursor = vim.api.nvim_win_get_cursor(0)
            local current_abs_line = state.offset + cursor[1] - 1
            
            if current_abs_line <= 0 then 
                return 
            end

            local start_line = current_abs_line - 1
            local found_line = tonumber(lib.log_engine_search_backward(state.engine, state.last_query, start_line))

            if found_line >= 0 then
                jump_to_line(bufnr, state, found_line)
            end
        end, { buffer = bufnr, silent = true })
    end

    vim.api.nvim_create_autocmd("BufWipeout", {
        buffer = bufnr,
        callback = function()
            local state = _G.JuanLogStates[bufnr]
            if state and state.timer then
                state.timer:stop()
                state.timer:close()
            end
            lib.log_engine_free(engine)
            _G.JuanLogStates[bufnr] = nil
        end
    })
end

function M.setup(user_config)
    if user_config then config = vim.tbl_extend("force", config, user_config) end

    vim.api.nvim_create_autocmd("BufReadCmd", {
        pattern = config.patterns,
        callback = function(ev)
            local file = vim.fn.expand("<amatch>:p") -- absolute path
            local stat = vim.loop.fs_stat(file)

            if not stat or stat.type == "directory" then
                return
            end

            -- hijack huge files, pass small ones to standard vim
            if stat.size > config.threshold_size then
                vim.schedule(function()
                    if vim.api.nvim_buf_is_valid(ev.buf) then
                        M.attach_to_buffer(ev.buf, file)
                    end
                end)
            else
                vim.schedule(function()
                    if not vim.api.nvim_buf_is_valid(ev.buf) then return end
                    
                    vim.api.nvim_buf_call(ev.buf, function()
                        local was_modifiable = vim.api.nvim_buf_get_option(ev.buf, 'modifiable')
                        vim.api.nvim_buf_set_option(ev.buf, 'modifiable', true)
                        
                        -- fallback: just read it normally
                        vim.cmd('silent! read ' .. vim.fn.fnameescape(file))
                        vim.cmd('1delete _')
                        
                        vim.api.nvim_buf_set_option(ev.buf, 'modified', false)
                        vim.api.nvim_buf_set_option(ev.buf, 'modifiable', was_modifiable)
                    end)
                end)
            end
        end
    })
end

return M
