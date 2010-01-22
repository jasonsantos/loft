package.path = ';;../../source/?.lua'

require 'list'

--TODO: create a proper test suite


l = list.create{
	1, 2, 3, 4, 5, 6, 7,
}


while l:hasNext() do
	print(l:next())
end

l:reset() 
print'reset'

print(l+3)

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
	mode=MODE.PAGE,
	load=function(idx)  return t[idx] end,
	onPageLoad=function() print'Page loaded' end
}

print(l.count, 'items')

for o in l do
	print(o)
end
