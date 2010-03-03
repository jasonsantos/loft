module(..., package.seeall)

description = [[MySQL Module for Database Behaviour]]

local base = require "loft.providers.base"

base.field_types.medium_text={type='MEDIUMTEXT', onEscape=string_literal}

base.database_type = 'mysql'

base.reserved_words {
	'and',
	'fulltext', 
	'table'
}

return base