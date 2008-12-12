package.path = [[;;./net.luaforge.loft/source/lua/5.1/?.lua;./net.luaforge.loft/source/lua/5.1/?/init.lua;]]
local now = os.clock()
print(now)
local provider = require"loft.providers.serialization"
provider.options{
	PERSISTENCE_PATH = './net.luaforge.loft/testsuite/db/${fileType}/${typeName}/';
}

local NUM_OF_OBJECTS_TO_GENERATE = 100

-- generates an ID
local id = provider.getNextId('test')
local firstId = id
local times = {}

for i=1, NUM_OF_OBJECTS_TO_GENERATE do
	
	-- persists a given table
	provider.persist('test', id, {
		id=id; 
		name='test'..i; 
		address= (i*347) .. ', Elm Street';
	})
	-- counts how many times this particular ID has been generated
	times[id] = (times[id] and (times[id] + 1)) or 1
	local id = provider.getNextId('test')
	-- generates the next ID
	id = provider.getNextId('test') 
end
 
local generatedId = firstId
table.foreachi(times, function(k, v)
	assert(k == generatedId, 'generated Id is out of sequence') 
	assert(v == 1, 'generated Id is duplicated')
	generatedId = generatedId + 1
end)

local lastId = firstId + NUM_OF_OBJECTS_TO_GENERATE

assert(id==lastId, 'wrong number of objects created')



local t = provider.retrieve('test', lastId)
assert(t, 'object was not retrieved for id==' .. lastId)

print(t.name)

print(id, os.clock()-now)