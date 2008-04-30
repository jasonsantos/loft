local 
	global, table, string
= 		_G, table, string

local 
	error, setmetatable, rawget, rawset, print, getfenv, setfenv, typeOf
= 	error, setmetatable, rawget, rawset, print, getfenv, setfenv, type 

module"schema"

local _ = function(o, property, defaultValue)
	rawset(o, property, rawget(o, property) or defaultValue or {})
	return rawget(o, property)
end

local _isType = function(s)
	return s and string.len(s) > 1 and string.sub(s, 1, 1) == string.upper(string.sub(s, 1, 1))
end

local Field = function(fieldName, type)
	local field = {
		['.fieldName'] = fieldName;
	}
	
	local methods = {
		addProperty = function(field, propertyName, value)
			field[propertyName] = value
		end;
	}
	
	return setmetatable({}, {
		__index = function(field, key)
			print(fieldName, key)
			return function(...)
				print('type:', key, ...)
			end
		end;
		
		__call = function(field, ...)
				print('called(from field):', ...)
		end
	})
end

global['Type'] = function(typeName)
	print('------------ type ' .. typeName .. ' --------------')
	local type = {
		['.type'] = type;
		['.typeName'] = typeName;
	}
	
	local lastCall = ''
	local lastField = ''
	
	return setmetatable(type, {
		__index = function(type, fieldName)
			 print('indexed:' .. fieldName) 
			 local fieldType
			 if _isType(fieldName) then
			 	lastCall = fieldName
			 	fieldType = fieldName
			 else
			 	lastField = fieldName			 	
			 end
			 
		 	local field =  _(_(type, '.fields'), lastField, Field(lastField, type))
		 	field['.fieldType'] = fieldType
			 
			return type, field
		end;
		
		__newindex = function(type, fieldName, defaultValue)
			rawset(type, fieldName, defaultValue)
		end;
		
		__call = function(fn, argument, ...)
			print('called (from type): '.. lastCall .. ' for field ' .. lastField  , ...)
			
			local firstChar = string.sub(lastCall, 1, 1)
			local args = {...} 

			local operation = ({
				['string'] = function()
					print('String argument')
					local field = _(_(type, '.fields'), lastField)
					field['.fieldName'] = argument
					return field
				end;
				['number'] = function()
					print('Number argument')
					local field = _(_(type, '.fields'), lastField)
					field['.fieldSize'] = argument
					return field
				end;
				['table'] = function()
					print('Table argument')
					local field = _(_(type, '.fields'), lastField)
					field['.subType'] = argument
					return field
				end;
				
			})[typeOf(argument)] 	-- selects the function 
									-- according to the type of argument
												
			return type, operation() -- executes the function selected
		end
	})
end

global['Schema'] = function(schemaName)
	-- TODO: ???
	return setmetatable(global, {  
		__call = function(schema, schemaTable)
			return schemaTable
		end
	})
end
return global['Schema']