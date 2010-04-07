module(..., package.seeall)

require 'lpeg'

function table.copy(t)
	local other = {}
	for k,v in pairs(t) do
		other[k]=v
	end
	return other
end

function table.add(first, second)
	local second = second or {}
	for k,v in pairs(second) do
		first[k]=v
	end
	return first
end

function table.merge(first, second)
	local other = {}
	table.add(other, first)
	table.add(other, second)
	return other
end

function table.count(t)
	local n = 0
	for _ in pairs(t) do
		n = n + 1
	end
	return n
end

WHITESPACE = (lpeg.S" \t\n\r")^0

function string.split(s, sep)
	sep = lpeg.P(sep)
	local elem = lpeg.C((1-sep)^0)
	local p = elem * (sep * elem)^0
	return lpeg.match(p, s)
end

local indexed_table_mt = {
	__newindex=function(t,k,v)
		if not tonumber(k) then
			table.insert(t, k)
		end
		rawset(t,k,v)
	end,
	__call=function(t,items)
		for _,v in ipairs(items) do
			t[v]=true
		end
		return t
	end
}

function indexed_table(o)
	local t = setmetatable({}, indexed_table_mt)
	return t(o or {})
end

function split_field_name(name)
	local names = {name:split('_')}
	names = type(names)=='string' and {names} or names or {}
	local attribute = table.remove(names,#names)
	return names, attribute
end

function op_field(v)
	local d = string.sub(string.gsub(v,'[^+%-]*', ''), 1, 1)
	local f = string.gsub(v,'[+%-]*', '')
    return d, f
end

