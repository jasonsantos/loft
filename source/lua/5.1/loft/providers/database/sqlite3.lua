----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 
module(..., package.seeall)

-----------------------------------------------
-- API
-----------------------------------------------
-- All persistence providers must implement 
-- this API to respond to the Loft engine
-----------------------------------------------
-- supportSchemas();

-- options(optionsTable);

-- getNextId([typeName]);

-- persist(class, id, data)
-- retrieve(class, id)

-- erase(class, id)

-- persistSimple(id, data)
-- retrieveSimple(id)

-- eraseSimple(id)

-- search(class, filter, visitorFunction)


-----------------------------------------------
-- OPTIONS
-----------------------------------------------
---  this provider default options
  
local defaultOptions = {
	PERSISTENCE_PATH = './db/';
	PERSISTENCE_FILENAME = 'persistence.db3';
	ISOLATE_IDS_PER_TYPE = true;
}



-----------------------------------------------
-- IMPLEMENTATION
-----------------------------------------------

-- requires

pcall(require,"luarocks.require")



------------------------------------------------------
-- API
------------------------------------------------------



-- supportSchemas();
--- respond whether this provider supports schemas
-- @return true if schemas are supported
function supportSchemas()
	return true
end


-- getNextId([typeName]);
--- returns the next ID to be used in an object.
-- must persists series of IDs to be able to respond
-- without the need to write immediately to persistent media.
-- optionally, some providers can have their IDs isoladed by typeName.
-- @param typeName	(optional) If the provider supports, it is possible
-- 					to obtain next ID for a specific type of object
-- @return ID of the next object to be created 

getNextId = (function ()

local aNextId = {}
local aIdsLeft = {}
local seriesSize = 20

local getCache = getCache

-- TODO: see that we do not lose one ID per series

return function(class)
	-- if options are set to isolate IDs per type use typename
	local typeName = options.ISOLATE_IDS_PER_TYPE and getTypeName(class)
	typeName = typeName or 'Ids'
	
	local idsLeft = aIdsLeft[typeName]
	local nextId = aNextId[typeName]
	
	-- if there's still IDs in the series at hand
	if idsLeft and aIdsLeft[typeName] > 0 then
		aNextId[typeName] = nextId + 1
		aIdsLeft[typeName] = idsLeft - 1 
		return nextId
	end

	-- get a new series'
	beginTransaction(typeName)
	
	-- save latest id + size of the series into persistence
	doSql([[update %s set lastId = lastId + %d + 1]], typeName, seriesSize)
	
	-- update memory id series
	local cursor = doSql([[select lastId from %s]], typeName)
	
	lastId = cursor[1] - seriesSize
	
	commit(typeName)
	
	aNextId[typeName] = lastId + 1 
	aIdsLeft[typeName] = seriesSize 
	
	return lastId
end

end)()

-- options([optionTable])
--- every provider must have a public 'options' 
-- table. It must implement a metatable to allow
-- this table to be called as a function

-- @param optionTable 	when called with a table parameter, 
--				 		it sets all parameters present in the
--						table with new values  
options = setmetatable({}, {
	__index = defaultOptions;
	__call = function(f, optionTable)
		if optionTable then
			assert(type(optionTable)=='table', 'options argument must be a table')
			table.foreach(optionTable, function(key, value)
				options[key] = value
			end)
		end
		return options 
	end;
})

-- persist(class, id, data)
-- retrieve(class, id)

-- erase(class, id)

-- persistSimple(id, data)
-- retrieveSimple(id)

-- eraseSimple(id)

-- search(class, filter, visitorFunction)