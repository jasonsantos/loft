
module(..., package.seeall)

description = [[Generic Module for Database Behaviour]]

field_types = {
	key={type='BIGINT', size='8', required=true, primary=true, autoincrement=true, onEscape=tonumber},
	integer={type='INT', size='5', onEscape=tonumber},
	number={type='DOUBLE', onEscape=tonumber},
	currency={type='DECIMAL', size={14,2}, onEscape=tonumber},
	text={type='VARCHAR',size='255', onEscape=stringLiteral},
	long_text={type='LONGTEXT', onEscape=stringLiteral},
	timestamp={type='DATETIME'}, 
	boolean={type='BOOLEAN'},
}

reserved_words = {
	'and', 'fulltext', 'table'
}

filters = {
	
	like = function(s) 
		return string.gsub(s or '', '[*]', '%%')
	end,
	
	contains =function(f,s) 
		return string.format("CONTAIS(%s, %s)", f, s)
	end,

}

quotes = [[']]

escapes = {
	quotes = function(s) 
		return string.gsub(s or '', "'", "''")
	end,
	
	new_lines = function(s) 
		return string.gsub(s or '', "\n", "\\n")
	end,
	
	reserved_field_name =function(s)
		return string.format('`%s`', s) 
	end
	
}

sql = {
	
	CREATE = [==[CREATE TABLE IF NOT EXISTS $tableName ( 
			$columns{", "}[=[$columnName $type$if{size}[[($size)]]$if{$primary}[[ PRIMARY KEY]]$if{$required}[[ NOT NULL]]$if{$description}[[ COMMENT $description]]$if{$autoincrement}[[ AUTO_INCREMENT]]]=]
	)]==],
		
}


local escapeFieldName=function(s)
	return reserved_words[string.lower(s)] and escapes.reserved_field_name(s) or s
end

local stringLiteral=function(s) 
	return quotes .. escapes.quotes(escapes.new_lines(s)) .. quotes
end

local passoverFunction = function(v) return v end


--- API

persist = function (entity, id, obj)

end

create = function (entity)

end

delete = function (entity, id, obj)
	
end

retrieve = function (entity, id)
	
end

search = function (entity, filters, pagination, sorting, visitorFunction)
	
end
