local consistenthash = require( "consistenthash" )

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


function test_no_dup()
    local chash = consistenthash:new( servers )
    local n = 1024 * 1024

    for i = 0, n do
        local a, b, c = chash:get(tostring( i ), 3)
        assert(a ~= b, 'a b')
        assert(b ~= c, 'b c')
        assert(c ~= a, 'c a')
    end
end


function test_distribution()

    local chash = consistenthash:new( servers )
    local n = 1024 * 1024

    local count = {}

    for i, server in ipairs(servers) do
        count[ server ] = 0
    end

    for i = 0, n do
        local a, b, c = chash:get(tostring( i ), 3)
        count[ a ] = count[ a ] + 1
        count[ b ] = count[ b ] + 1
        count[ c ] = count[ c ] + 1
    end

    for server, num in pairs(count) do
        -- print( server .. ' = ' ..  tostring( num ) )
    end

    local min = n
    local max = 0
    for server, v in pairs(count) do
        if min > v then
            min = v
        end
        if max < v then
            max = v
        end
    end

    print('min', min, 'max', max)
    assert((max-min)/ max < 0.2)
    assert((max-min)/ max > 0)
end


function test_basic()
    local chash = consistenthash:new( servers )

    local count = {}

    for i, server in ipairs(servers) do
        count[ server ] = 0
    end

    local a, b, c
    a, b, c = chash:get( '1', "3" )
    print( a )
    print( b )
    print( c )
    assert(count[a] ~= nil, 'a is in server table')
    assert(count[b] ~= nil, 'b is in server table')
    assert(count[c] ~= nil, 'c is in server table')
end


test_basic()
test_distribution()
test_no_dup()
