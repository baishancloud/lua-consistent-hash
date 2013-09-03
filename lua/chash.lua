local ch = require("chash_core")
module("chash")


function next(self)
	local p = self.h:next()
	return self.nodes[p+1]
end

function new(nodes)
	local h = ch:new(nodes)
	if h == nil then
		return nil
	end

	return {
		h			= h,
		nodes		= nodes,
		get			= function (self, key)
						local p = self.h:get(key)
						return self.nodes[p+1]
					end,

		next		= next,

		getn		= function (self, key, n)
						local t = {}
						local f = self.h:get(key)
						t[1] = sefl.nodes[f+1]
						for i=2, n, 1 do
							t[i] = next(self)
						end
						return t
					end
	}
end

