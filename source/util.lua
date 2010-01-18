function table.copy(t)
	local other = {}
	for k,v in pairs(t) do
		other[k]=v
	end
	return other
end

function table.add(first, second)
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

