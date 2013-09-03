local string = require("string")

local conf = {
	'10.75.27.83:80', '10.75.27.84:80', '10.77.120.21:80', '10.77.120.23:80'
}

--[[
chash = require("chash_core")
local ch = chash:new(conf)
--local ch = chash:new({})

for i=0, 999, 1 do
	local key = string.format("key_%d", i)
	local sv = ch:get(key)
	print(conf[sv+1])
end
]]--

local chash = require("chash")
local ch = chash.new(conf)

for i=0, 999, 1 do
	local key = string.format("key_%d", i)
	print(ch:get(key))
end
