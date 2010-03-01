module(..., package.seeall)

description = [[SQLITE3 Module for Database Behaviour]]

local base = require "loft.providers.base"

base.sql.CREATE = [==[
CREATE TABLE IF NOT EXISTS $table_name ( 
  $columns{", "}[=[$escape_field_name{$column_name} $type$if{$size}[[($size)]]$if{$primary}[[ PRIMARY KEY]]$if{$required}[[ NOT NULL]]$if{$autoincrement}[[ AUTOINCREMENT]]$sep
]=])
]==]

base.sql.INSERT = [[INSERT INTO $table_name ($data{", "}[=[$escape_field_name{$column_name}$sep]=]) VALUES ($data{", "}[=[$value$sep ]=])]]

base.field_types.key={type='INTEGER', primary=true, autoincrement=true, required=false, onEscape=tonumber}


base.database_engine.get_last_id = function(connection)
	return connection:getlastautoid()
end

return base