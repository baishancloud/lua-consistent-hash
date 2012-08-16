local tt = require( "consistenthash" )

local servers = {
    "a.0", "b.0", "c.0", "d.0", "e.0", "f.0", "g.0", "h.0", "i.0",
    "a.1", "b.1", "c.1", "d.1", "e.1", "f.1", "g.1", "h.1", "i.1",
    "a.2", "b.2", "c.2", "d.2", "e.2", "f.2", "g.2", "h.2", "i.2",
    "a.3", "b.3", "c.3", "d.3", "e.3", "f.3", "g.3", "h.3", "i.3",
    "a.4", "b.4", "c.4", "d.4", "e.4", "f.4", "g.4", "h.4", "i.4",
    "a.5", "b.5", "c.5", "d.5", "e.5", "f.5", "g.5", "h.5", "i.5",
    "a.6", "b.6", "c.6", "d.6", "e.6", "f.6", "g.6", "h.6", "i.6",
    "a.7", "b.7", "c.7", "d.7", "e.7", "f.7", "g.7", "h.7", "i.7",
    "a.8", "b.8", "c.8", "d.8", "e.8", "f.8", "g.8", "h.8", "i.8",
    "a.9", "b.9", "c.9", "d.9", "e.9", "f.9", "g.9", "h.9", "i.9",
}

local i, server, s2
local server_count = {}

for i, server in ipairs(servers) do
    server_count[ server ] = 0
end

local x = tt:new( servers )

function main()
    return tt.get, x, "123"
end


table.remove( servers, 1 )
-- segment fault passing nil
local y = tt:new( servers )

i = 1
n = 1024 * 1024
-- n = 100
inc = 0
while i<=n do
    server = tt.get( x, tostring( i ) )
    -- s2 = y:get( i )
    -- if s2 ~= server then
    --     inc = inc + 1
    --     -- print( inc )
    -- end
    -- server_count[ server ] = server_count[ server ] + 1
    -- print( server )
    i = i + 1
end
print( inc )

-- for server, num in pairs(server_count) do
--     print( server .. ' = ' ..  tostring( num ) )
-- end
