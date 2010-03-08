module(..., package.seeall)

local api = {}

local criteria_mt = {
	__index = api
}

--- Creates a criteria table from an entity
-- this function is executed when a search is preparing
-- the SQL will be generated from the resulting table 
function create(engine, entity, include_fields, exclude_fields)
	local criteria =setmetatable({}, criteria_mt)
	criteria.__engine = engine;
	criteria.__entity = entity;
	criteria.__columns = {};
	criteria.__fields = {};
	
	criteria.__include_fields = util.indexed_table(include_fields or {})
	criteria.__exclude_fields = util.indexed_table(exclude_fields or {})
	
	criteria.table_name = entity['table_name'] or entity['name']
	
	for field_name, field in pairs(entity.fields) do
		if not field.virtual and field.column_name then
			if not criteria.__exclude_fields[field_name] and ((not include_fields) or criteria.__include_fields[field_name]) then
				criteria:field(field_name, field)
			else
				criteria:column(field_name, field)
			end
		end
	end
	
	criteria:sort_fields(function(f1,f2) return f1.name < f2.name end)
	criteria:sort_fields(function(f1,f2) return f1.order < f2.order end)
	
	return criteria
end

function api.get_engine(criteria)
	return criteria.__engine;
end

function api.get_provider(criteria)
	return criteria:get_engine().provider;
end

function api.columns(criteria, fields)
	for field_name,field in pairs(fields) do
		criteria:column(field_name,field)
	end
	return criteria
end
 
function api.column(criteria, field_name, field)
	local provider = criteria:get_provider();
	local field_type = provider.field_types[field.type] or {}
	local column = table.merge(field_type, field)
	
	column.name = field.column_name
	column.alias = field_name
	column.order = field.order or 999
	column.type = field_type.type
	
	criteria.__columns[field_name] = column 
	
	table.insert(criteria.__columns, criteria.__columns[field_name])
	
	return criteria
end

function api.field(criteria, field_name, field)
	
	if field then
		criteria:column(field_name, field)
	end
	
	criteria.__fields[field_name] = criteria.__columns[field_name]
	table.insert(criteria.__fields, criteria.__columns[field_name])
	
	return criteria
end

function api.sort_fields(criteria, fn)
	table.sort(criteria.__columns, fn)
	table.sort(criteria.__fields, fn)
	return criteria
end

function api.include_fields(criteria, fields)
	criteria.__include_fields(fields)
	return criteria			
end

function api.exclude_fields(criteria, fields)
	criteria.__exclude_fields(fields)
	return criteria			
end
 