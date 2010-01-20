package.path = ';;../../source/?.lua'

require 'proxy'

do
	assert(proxy)
end

do
	assert(proxy.create)
	assert(proxy.get_object)
end

do
	local pxy1 = proxy.create('Person', nil, {name="Ian Chesterton"})
	assert(pxy1['name']=="Ian Chesterton")
	assert(rawget(pxy1,'name')==nil)
end

do
	local pxy1 = proxy.create('Person', 1, {name="Sarah Jane Smith"})
	assert(pxy1['name']=="Sarah Jane Smith")
	assert(rawget(pxy1,'name')==nil)
	
	local pxy2 = proxy.create('Person', 1)
	assert(pxy2.name=="Sarah Jane Smith")
	assert(pxy1~=pxy2)
	assert(getmetatable(pxy1).__obj == getmetatable(pxy2).__obj)
end

do
	local classPerson = {'id', 'name'} -- fake schema
	local obj = {id=1, name="Rose Tyler"}
	local obj1 = {id=2, name="Martha Jones"}
	local obj2 = {id=3, name="Donna Noble"}
	local obj3 = {name="Jack Harkness"}
	
	local p1 = proxy.create(classPerson, nil, obj1)
	local p2 = proxy.create(classPerson, nil, obj2)
	local p3 = proxy.create(classPerson, nil, obj3)
	
	assert(p1.name=="Martha Jones")
	assert(p2.name=="Donna Noble")
	assert(p3.name=="Jack Harkness")
	
	assert(getmetatable(p3).__id[1]=='new')
	
	p3.id = 4

	assert(getmetatable(p3).__id==4)

end

do
	local p1 = proxy.create{'person'} -- no id, no object
	assert(p1==nil)
end