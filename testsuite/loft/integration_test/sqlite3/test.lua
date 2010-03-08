package.path = package.path .. ';../../../../source/?.lua'
local loft = require 'loft'
local schema = require 'schema'

local loft_instance = loft.engine({
	database = "test.sqlite3",
	database_type = "sqlite3",
	provider = "sqlite3"
})

local default = schema.expand(function ()
	column_prefix = "f_"

	test = entity {
		table_name= "T_Test",
		fields = { 
			id = { order = 1, column_name = "f_infoid", type = "key"},
			name = { order=2, type = "text", size = 100, maxlength=250 },
			address = { order=2, type = "text", size = 100, maxlength=250 }	
		},
		handlers = {
	        before_save = function(e, obj) record.lastLog = 'Saving Entity' end,
	    },
	}
	
end)

local model = loft_instance.decorate(default)

local now = os.clock()
local log = function()
	print('log: ' .. os.clock() - now)
end

local lfs = require'lfs'

local NUM_OF_OBJECTS_TO_GENERATE = 100

-- generates an ID
local times = {}
local duplicatesTimes = {}
local firstId
local id

model.test:create()

local items = loft_instance.find(default.entities.test)

for _, item in ipairs(items) do
	assert(item:destroy())
end
 
for i=1, NUM_OF_OBJECTS_TO_GENERATE do
	-- persists a given table
	local l = model.test:new()
	l.name = 'test'..i; 
	l.address = (i*347) .. ', Elm Street';
	l:save()
	id = l.id
		
	if (not firstId ) then
		firstId = id
	end
	
	-- counts how many times this particular ID has been generated
	table.insert(times, id)
	duplicatesTimes[id] = (duplicatesTimes[id] or 0) + 1
end
 
local generatedId = firstId

for k, v in ipairs(times) do
	assert(v == generatedId, 'generated Id is out of sequence') 
	assert(duplicatesTimes[v] == 1, 'generated Id is duplicated')
	generatedId = generatedId + 1
end

local lastId = firstId + NUM_OF_OBJECTS_TO_GENERATE - 1

assert(id==lastId, 'wrong number of objects created')

local t = model.test:get(lastId)

local t2 = model.test:find{filters = {id = lastId}}[1]

assert( t.id == t2.id, 'check if id is the same in get and find')
assert( t.name == t2.name, 'check if name is the same in get and find' )

assert(t.id, 'object was not retrieved for id==' .. lastId)

assert(t.name == 'test' .. (lastId-(firstId-1)), 'The proper object was not retrieved')

local list = model.test:find{filters = {name='test99'}}

for _,item in ipairs(list) do
	assert(item.name=='test99')
end

assert(#list==1, 'search did not find the item by name')

for _,item in ipairs(list) do
	assert(item:destroy(), "unable to destroy item")
end

list = model.test:find{
	filters = {name='test99'}, 
	visitor = function(item)
		assert(item.name=='test98')
		table.insert(list, item)
	end
}

assert(#list==0, 'erase did not remove the item properly')

list = model.test:find{filters = {name='test98'}}

assert(#list==1, 'short search did not find the item by name')

assert(list[1]:destroy(), "unable to destroy item")

list = model.test:find{filters = {name='test98'}}

assert(#list==0, 'erase did not remove the item properly')

local NumLimit = 2
for i=1, NUM_OF_OBJECTS_TO_GENERATE / NumLimit do
	list = model.test:find( { 
		pagination = {
			offset = i - 1,
			limit = NumLimit
		},
		sorting = {"+id"}
	 })
	 table.foreachi(list, function (v, t)
		assert(t.name == "test" .. i)
		i = i + 1
	 end)
end

_count = model.test:count( {filters = {name='test98'}})

assert(_count==0, 'erase did not remove the item properly')

assert( model.test:count( {filters = {name='test1'}}) == 1)

list = model.test:find( { sorting = {'-name'}, pagination = {limit=10}})

assert(#list==10, 'limited search has brought the wrong amount of items')

assert( pcall( model.test.count, model.test, { filters = {COLUMN_WRONG='test1'}}) == false)

_count = model.test:count()

assert(_count, 'global search found the wrong amount of items')

list = model.test:find( { sorting = {'-name'}})

assert(#list==98, 'global search found the wrong amount of items')

for _,item in ipairs(list) do
	assert(item:destroy() ~= false, "Don't search and destroy objects loft")
end

list = model.test:find()

assert(#list==0, 'erase did not remove all items properly')

print 'OK'