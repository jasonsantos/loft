package.path = [[;;./net.luaforge.loft/source/lua/5.1/?.lua;./net.luaforge.loft/source/lua/5.1/?/init.lua;]]
local now = os.clock()
print(now)
local provider = require"loft.providers.serialization"

local id
local times = {}
for i=1, 100 do
	-- generates an ID
	id = provider.getNextId('test')
	-- persists a given table
	provider.persist('test', id, {
		id=id; 
		name='test'..i; 
		address= (i*347) .. ', Elm Street';
	})
	-- counts how many times this particular ID has been generated
	times[id] = (times[id] and (times[id] + 1)) or 1 
end
--[[
local objs = {}

for i=1, 1000 do
	objs[i] = provider.retrieve('teste', i) 
end
]]
local generatedId = 1
table.foreachi(times, function(k, v)
	assert(k == generatedId) 
	assert(v == 1)
	generatedId = generatedId + 1
end)

assert(id==100)


local t = provider.retrieve('test', 100)

print(t.name)

print(id, os.clock()-now)