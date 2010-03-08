require 'util'

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
				criteria:field(field_name)
			else
				criteria:column(field_name)
			end
		end
	end
	
	criteria:sort_fields(function(f1,f2) return f1.name < f2.name end)
	criteria:sort_fields(function(f1,f2) return f1.order < f2.order end)
	
	return criteria
end

function api.get_entity(criteria)
	return criteria.__entity;
end

function api.get_engine(criteria)
	return criteria.__engine;
end

function api.get_schema(criteria)
	local engine = criteria:get_engine() or {}
	return engine.schema;
end

function api.get_all_entities(criteria)
	local schema = criteria:get_schema() or {}
	return schema.entities;
end

function api.get_provider(criteria)
	return criteria:get_engine().provider;
end

local function split_condition_name(name)
	local names = name:split('_')
	names = type(names)=='string' and {names} or names or {}
	local attribute = table.remove(names,#names)
	return names, attribute
end

local function create_column(criteria, field_name)
	local provider = criteria:get_provider();
	local entity = criteria:get_entity()
	
	-- find entity and column
	local relations, attribute_name = split_condition_name(field_name)
	local parent_relation
	for _, relation_name in ipairs(relations) do
		_, entity = criteria:join(relation_name, parent_relation)
		parent_relation = relation_name 
	end

	local field = assert(entity.fields[attribute_name], "Could not find field '" .. attribute_name .. "' in entity '" .. entity.name .. "'") 

	local field_type = provider.field_types[field.type] or {}
	local column = table.merge(field_type, field)
	
	column.name = field.column_name
	column.alias = field_name
	column.order = field.order or 999
	column.type = field_type.type

	column.entity = entity 
	column.entity_name = entity_name 
	
	return column
end

local function create_condition(criteria, field_name, field_condition)
	
	local left_side = create_column(criteria, field_name)
	local right_side
	
	if type(field_condition)=='string' or type(field_condition)=='number' or type(field_condition)=='boolean' then
		right_side = { type = 'simple', value = field_condition }
	elseif type(field_condition)=='table' and #field_condition>1 then
		right_side = { type = 'set', value = field_condition }
	else
		right_side = field_condition
		if type(field_condition)=='table' then
			local op,c = next(field_condition) 
			if type(c)=='table' and c.field then
				local column = create_column(criteria, c.field)
				table.foreach(column, print)
				right_side[op] = table.merge(right_side, column)
				table.foreach(right_side, print)
			end
		end
	end
	
	return {left_side=left_side, right_side=right_side}
end 

function api.conditions(criteria, filters)
	criteria.__conditions = criteria.__conditions or {}
	-- for each condition
	
	if ( type(filters) == "table" and next(filters)) then
		for lside, rside in pairs(filters) do
			table.insert(criteria.__conditions, create_condition(criteria, lside, rside))
		end
		--TODO: sort condition columns by order and name
	end
	
	return criteria, criteria.__conditions
end

function api.get_conditions(criteria)
	return criteria.__conditions or {}
end

function api.join(criteria, attr, left_alias)
	local entity = criteria:get_entity()
	local entities = criteria:get_all_entities() or {};
	
	criteria.__joins = criteria.__joins or {};
	
	local left_alias = left_alias or entity.name
	local right_attribute = assert(attr, 'join must have and attribute name')
	local right_field = assert(left.fields[right_attribute], "field '"..tostring(right_attribute).."' doesn't exist on entity '"..tostring(left_alias).."'")

	if not criteria.__joins[left_alias] then
		error("entity '"..tostring(left_alias).."' must be added to criteria before being used as the left side of a join",2)
	end
	
	local left = assert(entities[left_alias], "join entity '" .. tostring(left_alias).. "' must be present in schema") 

	local right_alias = assert(right_field.entity, "field '"..tostring(right_attribute).."' must be a relationship attribute on entity '"..tostring(left_alias).."'")

	local right = assert(entities[right_alias], "join entity '" .. tostring(right_alias).. "' must be present in schema") 
	
	if criteria.__joins[right_alias] then
		-- attribute is already on a previous join
		return criteria
	end
	
	--TODO: add support for left and right joins
	--TODO: add support for key names different than 'id'
	table.insert(criteria.__joins, {type="inner", join_table=right.table_name, alias=right_alias, on={ create_condition(criteria, left_alias..'_'..right_attribute, {field=right_alias..'_id'}) }}) 

	criteria.__joins[right_alias] = true
	
	return criteria, right
end

function api.columns(criteria, fields)
	for field_name,field in pairs(fields) do
		criteria:column(field_name,field)
	end
	return criteria, criteria.__columns
end

function api.column(criteria, field_name)
	
	criteria.__columns[field_name] = create_column(criteria, field_name) 
	
	table.insert(criteria.__columns, criteria.__columns[field_name])
	
	return criteria, criteria.__columns[field_name]
end

function api.field(criteria, field_name)

	if criteria.__fields[field_name] then
		return criteria, criteria.__fields[field_name] 
	end

	local _, column = criteria:column(field_name)
	
	criteria.__fields[field_name] = column
	table.insert(criteria.__fields, column)
	
	return criteria, column
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
 