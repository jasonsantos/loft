local debug = debug
local table = table
local print = print
local error = error
local type = type
local getfenv = getfenv
local pairs = pairs
local rawset = rawset
local rawget = rawget
local require = require
local setmetatable = setmetatable
local getmetatable = getmetatable
local os = { time = os.time }

module"loft"

-- the persistence provider engine to be used
local provider = false -- none 

--- initializes the persistence engine. 
-- sets the provider internally to be returned 
-- when needed by the function getProvider()
-- @param providerName 	the name of the persistence provider to be used
--						the provider correspond to a module named 'loft.providers.&lt;providerName&gt;'
-- @param optionsTable 	(optional) table with a set of options for the loft engine and the provider
function initialize(providerName, optionsTable)
	local providerName = providerName or "serialization"
	provider = require("loft.providers." .. providerName) 
	if not provider then
		error("Unable to initialize persistence provider " .. tostring(providerName))
	end
	
	if optionsTable then
		initialize(optionsTable.sourceName, unpack(optionsTable.connectionParameters or {}))
	
		provider.options(optionsTable)
	end
	
	return getfenv(1)
end

--- internal function to return the persistence provider
-- @returns provider object 
local function getProvider()
	if provider then
		return provider;
	else
		error"Persistence provider is not set"
	end
end

--- returns the provider's options table or sets values within it
-- @param optionTable 	when called with a table parameter, 
--				 		it sets all parameters present in the
--						table with new values  
-- @returns provider option table 
local function options(optionTable)
	if provider then
		return provider.options(optionTable);
	else
		error"Persistence provider is not set"
	end
end

--- table containing all Object Schemas in the application
-- schemas are not unloaded 
local allSchemas = {}

-- TODO: index arrays by typeName

--- table containing all Objects in the application.
-- objects are unloaded when all references are dead
local allObjects = {}
setmetatable(allObjects, { __mode="v" })

--- table containing timestamp of last refresh for all Objects.
local allRefreshTimestamps = {}

--- table containing indexes for faster memory search
-- TODO: store and update indexes on the proxy metatable
-- TODO: allow configuration to determine whether to use indexes
-- local allIndexes = {}

--- Create a schema associated to a type name
-- a Schema is a simple table with default values 
-- TODO: evolve schema to allow complex types and reference types
-- @param typeName 	name of the type, to be used when creating news instances
-- @param schema	table with default values for each field
function registerSchema(typeName, schema)
	if not schema or type(schema) ~= 'table' then
		error"Invalid schema"
	end
	
	if not allSchemas[typeName] then
		allSchemas[typeName] = schema
	else
		error"Schema already registered"
	end
end

--- Proxy metatable to be used on every object
-- intercepts all gets and sets and controls 'dirtyness' 
-- TODO: implement lazy instantiation for referece types
local __proxy_metatable = {
		__index=function(o, key)
			if key=='id' then
				return rawget(o, '__id') 
			end
			
			local attributes = rawget(o, '__attributes')
			-- TODO: load using findById for reference types
			return attributes[key]
		end;
		__newindex=function(o, key, value)
			local attributes = rawget(o, '__attributes')
			-- TODO: check type
			-- TODO: store ID for reference types
			attributes[key] = value		
			rawset(o, '__dirty', true)			
		end
	}

--- Creates a new instance of an object
-- alternatively, can turn a simple table into an object
-- can also be used to recreate objects once their data come from persistence
-- @param typeName	schema object or name of the type associated with the object
--					if no typeName is designated, a simple table object will be created
-- @param data 		(optional) table with data to be loaded into object
-- @param id 		(optional) ID of the object to be restored
-- @return new object of the designated type or a simple object
function new(typeName, data, id)
	local id = id or (data and data.id) or false
	local obj
	if not id then
		-- create a new object from a table
		id = getProvider().getNextId(typeName)
	end
	
	if typeName then
		-- it's a complex type.
		obj = {} 
		rawset(obj, '__id', id)
		rawset(obj, '__attributes', {})

		if getProvider().supportSchemas() then
			rawset(obj, '__typeName', typeName)
		end
		
		local class
		
		if type(typeName)=='table' then
			class = typeName
		else
			class = allSchemas[typeName]
		end 
				
		if class then
			
			for key, value in pairs(class) do
				-- TODO: separate default values from FieldTypes in schema
				-- TODO: register fieldTypes in object
				obj.__attributes[key] = value			
			end
			
			-- execute creation hook if present
			if class.__create then
				obj = class.__new(obj) 
			end
		end
		
		if data then
			for key, value in pairs(data) do
				-- we do not want to treat the ID as a changeable value
				if key ~= 'id' then 
					-- but every other attribute on table gets right into the attribute pool
					obj.__attributes[key] = value
				end
			end
			
		end
		
		setmetatable(obj, __proxy_metatable)

	else
		-- it's a simple type. was stored directly
		obj = data or {}
		obj.id = id
	end
	
	-- TODO: check if we really want to give IDs to simple types 	
	allObjects[id] = obj
	rawset(obj, '__dirty', true)
	allRefreshTimestamps[id] = os.time() -- this object was refreshed now
		
	-- TODO: return a proxy to ouside world, and keep the real object inside the pool, so we can refresh it when timeout is over an be able to replace eventual live references
	
	return obj
end

--- internal function to return the schema of a given object
-- @param obj object from which to get the schema from or a string with typeName
-- @returns schema object or null if object was not created with a schema 
local function getComplexType(obj)
	local typeName
	if type(obj) == 'string' then
		typeName = obj
	else
		typeName = rawget(obj, '__typeName')
	end

	if typeName then
		return allSchemas[typeName]
	end 
end

--- internal function to return the raw data of a given object
-- @param obj object from which to get the data from
-- @returns schema object or the object itself if object is not a complex type 
local function getRawData(obj)
	-- TODO: get recursive data for reference types
	return rawget(obj, '__attributes') or obj
end

--- Saves the object to the persistence.
-- if object has a complex type, saves to the appropriate repository
-- if object has a simple type, saves according to the object ID
-- @param obj object to be saved
-- @return boolean indicating whether the object needed to be saved or not
function save(obj)
	local class = getComplexType(obj)
	local id = obj.id
	local data = getRawData(obj)
	if class then
		-- TODO: save recursively for reference types
		if rawget(obj, '__dirty') then
			getProvider().persist(class, id, data)
			rawset(obj, '__dirty', false)
		else
			return false
		end
	else
		getProvider().persistSimple(id, data)
	end 
	return true
end


local function getObjectFromPool(id)
	local obj = allObjects[id]

	local lastrefreshtime = allRefreshTimestamps[id] and os.time() - allRefreshTimestamps[id]

	-- if object was refreshed in the last 2 seconds then return it
	if lastrefreshtime and lastrefreshtime < 2  then
		return  obj 	
	else
		-- TODO: invalidate the object being referenced by the application
	end
end

--- Finds an object by its ID.
-- if object is already in memory cache, it is obtained directly from there
-- if not, it will be loaded from persistence and restored to memory cache
-- @param id 		ID of the object to be retrieved
-- @param typeName 	the typeName of the object to be retrieved
-- @return 			object recovered
function findById(id, typeName)
	local obj = getObjectFromPool(id) 
	
	if obj then
		return obj
	end 
	
	local data
	local class = typeName and getComplexType(typeName)
	
	if class then
		data = getProvider().retrieve(class, id)
	else
		data = getProvider().retrieveSimple(id)
	end
	
	return new(class, data, id)
end

--- Finds all objects matching a given set of attribute filters.
-- fore each given object matching the criteria, if it is already 
-- in memory cache, it is obtained directly from there
-- if not, it will be loaded from persistence and restored to memory cache
--
-- @param filter 	table containing a set of filter conditions
--					
-- @param typeName 	the typeName of the objects to be retrieved
-- @param visitor	(optional) function to be executed 
-- 					every time an item is found in persistence
-- @return 			table with all objects recovered
function findAll(filter, typeName, visitor)
	local results = {}
	local class = getComplexType(typeName)
	
	local visitor = visitor or function(data)
		local id = data.id
		local obj
		
		if id then
			obj = getObjectFromPool(id)
			obj = obj or new(class, data, id)
		else
			obj = data
		end
		
		table.insert(results, obj)
	end
	
	getProvider().search(class, filter, visitor)
	
	return results
end

--- Destroys an object.
-- remove it from memory and persistence. 
--
-- @param obj object to be destroyed
-- @return true if object was successfully erased from persistence
function destroy(obj)
	local class = getComplexType(obj)
	local erased 

	allObjects[id] = nil
		
	if class then
		return getProvider().erase(class, id)
	else
		return getProvider().eraseSimple(id)
	end
	
end