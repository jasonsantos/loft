module(..., package.seeall)

description = [[MySQL Module for Database Behaviour]]

local base = require "loft.providers.base"


base.database_type = 'mysql'

base.reserved_words {
	'and',
	'fulltext', 
	'table'
}

return base