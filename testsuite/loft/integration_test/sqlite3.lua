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

	info = entity {
		table_name= "T_Info",
		fields = { 
			id = { order = 1, column_name = "f_infoid", type = "key"},
			title = { order=2, type = "text", size = 100, maxlength=250 },
	
		},
		handlers = {
	        before_save = function(e, obj) record.lastLog = 'Saving Entity' end,
	    },
	}
	
	section = entity { 
		fields = { 
			id = { order = 1, colum_name = "F_SectionID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}	
	
end)

--loft_instance.create(default.entities.info)

local info = loft_instance.new(default.entities.info)

info.title = "Alessandro"

loft_instance.save( info )

print( info.id )

--print( loft_instance.find(default.entities.info).items[2].title, print )
info.title = "Fabio"

loft_instance.save( info )

loft_instance.destroy( info )

print( loft_instance.count(default.entities.info) )

os.exit()


provider = require"loft.providers.database.sqlite3"
provider.options{
	PERSISTENCE_FILENAME = 'test.db3';
	PERSISTENCE_PATH= '/tmp/net.luaforge.loft/testsuite/'
}
provider.initialize('test.db3')

local now = os.clock()
local log = function()
	print('log: ' .. os.clock() - now)
end

local lfs = require'lfs'

local NUM_OF_OBJECTS_TO_GENERATE = 100

-- generates an ID
local id = provider.getNextId('test')
local firstId = id
local times = {}
local duplicatesTimes = {}

for i=1, NUM_OF_OBJECTS_TO_GENERATE do
	-- persists a given table
	provider.persist('test', id, {
		id=id; 
		name='test'..i; 
		address= (i*347) .. ', Elm Street';
	})
	-- counts how many times this particular ID has been generated
	table.insert(times, id)
	duplicatesTimes[id] = (duplicatesTimes[id] or 0) + 1
	id = provider.getNextId('test')
end
 
local generatedId = firstId
table.foreachi(times, function(k, v)
	assert(v == generatedId, 'generated Id is out of sequence') 
	assert(duplicatesTimes[v] == 1, 'generated Id is duplicated')
	generatedId = generatedId + 1
end)

local lastId = firstId + NUM_OF_OBJECTS_TO_GENERATE

assert(id==lastId, 'wrong number of objects created')

local t = provider.retrieve('test', lastId-1)
assert(t, 'object was not retrieved for id==' .. lastId)
assert(t.name == 'test' .. (lastId-firstId), 'The proper object was not retrieved')

local list = {}

provider.search('test', {name='test99'}, function(item)
	assert(item.name=='test99')
	table.insert(list, item)
end)

assert(#list==1, 'search did not find the item by name')

table.foreachi(list, function(i, item)
	provider.erase('test', item.id)
end)

list = provider.search('test', {name='test99'}, function(item)
	assert(item.name=='test99')
	table.insert(list, item)
end)

assert(#list==0, 'erase did not remove the item properly')

list = provider.search('test', {name='test98'})

assert(#list==1, 'short search did not find the item by name')

table.foreachi(list, function(i, item)
	provider.erase('test', item.id)
end)

list = provider.search('test', {name='test98'})

assert(#list==0, 'erase did not remove the item properly')


local NumLimit = 2
for i=1, NUM_OF_OBJECTS_TO_GENERATE / NumLimit do
	list = provider.search('test', { 
		__sort='+id',
		__offset = i - 1,
		__limit = NumLimit,
	 })
	 table.foreachi(list, function (v, t)
		assert(t.name == "test" .. i)
		i = i + 1
	 end)
end

_count = provider.count('test', {name='test98'})

assert(_count==0, 'erase did not remove the item properly')

assert( provider.count('test', {name='test1'}) == 1)

list = provider.search('test', {__sort='-name', __limit=10})

assert(#list==10, 'limited search has brought the wrong amount of items')

assert( provider.count('test', {COLUMN_WRONG='test1'}) == 0)

_count = provider.count('test')

assert(_count, 'global search found the wrong amount of items')

list = provider.search('test', {__sort='-name'})

assert(#list==98, 'global search found the wrong amount of items')

table.foreachi(list, function(i, item)
	provider.erase('test', item.id)
end)

list = provider.search('test', {})

assert(#list==0, 'erase did not remove all items properly')

print 'OK'