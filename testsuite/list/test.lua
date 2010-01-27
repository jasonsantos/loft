package.path = ';;../../source/?.lua'

require 'list'

do -- Module
	assert(list)
end

do -- API presence
	assert(list.MODE)
	assert(list.create)
end

do -- testing create API function 
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(l)
	assert(l.hasNext)
	assert(l.next)
	assert(l.reset)
end

do -- testing iteration methods
	local o = {1, 2, 3, 4, 5, 6, 7}
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(l)
	local i = 1
	while l:hasNext() do
		v = l:next()
		assert(v==o[i])
		i = i + 1
	end
end

do -- testing the reset method
	local o = {1, 2, 3, 4, 5, 6, 7}
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(l)
	local i = 1
	while l:hasNext() do
		v = l:next()
		assert(v==o[i])
		i = i + 1
	end	
	l:reset()
	i = 1
	while l:hasNext() do
		v = l:next()
		assert(v==o[i])
		i = i + 1
	end	
end

do -- testing the '+' opreator overload for next
	local o = {1, 2, 3, 4, 5, 6, 7}
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(l)
	local v = l+3
	assert(v==o[3])
	v = l-1 
	assert(v==o[2])
	v = l-1 
	assert(v==o[1])
end

do -- testing the call iterator
	local o = {1, 2, 3, 4, 5, 6, 7}
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(l)
	local v = l()
	assert(v==o[1])
	v = l()
	assert(v==o[2])
end

do -- testing the size of the list
	local o = {1, 2, 3, 4, 5, 6, 7}
	local l = list.create{
		1, 2, 3, 4, 5, 6, 7,
	}
	assert(#l==#o)
end

do return end

while l:hasNext() do
	print(l())
end

l:reset()
print'reset'

for o in l do
	print(o)
end

t= {}
for i = 33, 126 do
 table.insert(t, string.char(i))
end

l = list.create{
	count=#t,
	mode=list.MODE.PAGE,
	load=function(idx)  return t[idx] end,
	onPageLoad=function() print'Page loaded' end
}

print(l.count, 'items')

for o in l do
	print(o)
end
