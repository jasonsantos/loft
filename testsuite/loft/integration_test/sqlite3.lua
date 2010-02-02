package.path = package.path .. ';../../../source/?.lua'
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
loft_instance.create(default.entities.test)

table.foreachi(loft_instance.find(default.entities.test).items, function(i, item)
	assert(loft_instance.destroy( item, true ))
end)
 
for i=1, NUM_OF_OBJECTS_TO_GENERATE do
	-- persists a given table
	local l = loft_instance.new(default.entities.test)
	l.name = 'test'..i; 
	l.address = (i*347) .. ', Elm Street';
	loft_instance.save(l)
	id = l.id
	
	if (not firstId ) then
		firstId = id
	end
	
	-- counts how many times this particular ID has been generated
	table.insert(times, id)
	duplicatesTimes[id] = (duplicatesTimes[id] or 0) + 1
end
 
local generatedId = firstId
table.foreachi(times, function(k, v)
	assert(v == generatedId, 'generated Id is out of sequence') 
	assert(duplicatesTimes[v] == 1, 'generated Id is duplicated')
	generatedId = generatedId + 1
end)

local lastId = firstId + NUM_OF_OBJECTS_TO_GENERATE - 1

assert(id==lastId, 'wrong number of objects created')

--~ local t = loft_instance.get(default.entities.test, lastId)

--~ local t2 = loft_instance.find(default.entities.test, {filters = {id = lastId}})

--~ assert( t.id == t2.id, 'check equals data in get and find')
--~ assert( t.name == t2.name, 'check equals data in get and find' )

--~ assert(t.id, 'object was not retrieved for id==' .. lastId)
--~ assert(t.name == 'test' .. (lastId-firstId), 'The proper object was not retrieved')

local list = {}

table.foreachi(loft_instance.find(default.entities.test, {filters = {name='test98'}}).items, function(i, item)
	assert(item.name=='test98')
	table.insert(list, item)
end)

assert(#list==1, 'search did not find the item by name')

table.foreachi(list, function(i, item)
	loft_instance.destroy( item )
end)

list = loft_instance.find(default.entities.test, {filters = {name='test98'}, visitor = function(item)
	assert(item.name=='test98')
	table.insert(list, item)
end})

assert(#list==0, 'erase did not remove the item properly')

list = loft_instance.find(default.entities.test, {filters = {name='test98'}}).items

assert(#list==1, 'short search did not find the item by name')

loft_instance.destroy( list[1], true )

list = loft_instance.find(default.entities.test, {filters = {name='test98'}}).items

assert(#list==0, 'erase did not remove the item properly')

local NumLimit = 2
for i=1, NUM_OF_OBJECTS_TO_GENERATE / NumLimit do
	list = loft_instance.find(default.entities.test, { 
		pagination = {
			offset = i - 1,
			limit = NumLimit
		},
		sorting = {"+id"}
	 }).items
	 table.foreachi(list, function (v, t)
		assert(t.name == "test" .. i)
		i = i + 1
	 end)
end

_count = loft_instance.count(default.entities.test, {filters = {name='test98'}})

assert(_count==0, 'erase did not remove the item properly')

assert( loft_instance.count(default.entities.test, {filters = {name='test1'}}) == 1)

list = loft_instance.find(default.entities.test, { sorting = {'-name'}, pagination = {limit=10}})

assert(#list==10, 'limited search has brought the wrong amount of items')

assert( pcall( loft_instance.count, default.entities.test, { filters = {COLUMN_WRONG='test1'}}) == false)

_count = loft_instance.count(default.entities.test)

assert(_count, 'global search found the wrong amount of items')

list = loft_instance.find(default.entities.test, { sorting = {'-name'}}).items

assert(#list==99, 'global search found the wrong amount of items')

table.foreachi(list, function(i, item)
	assert(loft_instance.destroy( item, true ) ~= false, "Don't search and destroy objects loft")
end)

list = loft_instance.find(default.entities.test).items

assert(#list==0, 'erase did not remove all items properly')

print 'OK'