module('list', package.seeall)

local listmetadata = setmetatable({}, {__mode='k'})

MODE = {
	PAGE = 'page',
	ITEM = 'item'
}

local List

--- finds pagenumber based on page size
local function findPage(idx, pageSize)
	return math.ceil(idx/ pageSize), ((idx-1)% pageSize)+1
end

local function setCurrentItem(o, idx)
	o.position = idx
	
	if idx<1 or idx>o.count then
		o.currentItem = nil
		o.loaded = true
		return
	end
	
	if o.mode == MODE.PAGE then
		local pageNo, offset = findPage(idx, o.pageSize)
		local page
		if o.pages[pageNo] then
			page = o.pages[pageNo]
		else
			-- loads the page
			page = o.loadPage(pageNo, l)
			
			if o.onPageLoad then
				page = o.onPageLoad(page, pageNo, l) or page
			end
			
			-- add it to the cache of loaded pages
			o.pages[pageNo] = page

			-- control number of loaded pages in the cache
			o.pages.loaded = o.pages.loaded or {}
			table.insert(o.pages.loaded, pageNo)
			if #o.pages.loaded > o.maxLoadedPages then
				-- when number of pages overflows, removes the oldest loaded page
				local p = table.remove(o.pages.loaded, 1)
				o.pages[p] = nil
			end
		end
		o.currentItem = page[offset]
		o.loaded = true
	else
		o.currentItem = o.load(idx, l)
		if o.onLoad then
			o.currentItem = o.onPageLoad(o.currentItem, idx, l) or o.currentItem
		end
		
		o.loaded = true
	end
end

List = {

	__index = function(l, idx)
		if tonumber(idx) then
			return List.moveTo(l, idx)
		end
		
		if List[idx] then
			return List[idx]
		end
		
		if listmetadata[l][idx] then
			return listmetadata[l][idx]
		end
		
		return rawget(l, idx)
	end,
	
	__add = function(l, n)
		return List.next(l, n)
	end,
	
	__sub = function(l, n)
		return List.previous(l, n)
	end,
	
	__call = function(l)
		return l:next()
	end,
	
	moveTo=function(l, idx)
		local o = listmetadata[l]
		if not o.loaded or idx~=o.position then
			setCurrentItem(o, idx)
		end
		return o.currentItem
	end,
	
	next=function(l, n)
		local o = listmetadata[l]
		local n = n or 1
		return List.moveTo(l, o.position+n)
	end,
	
	previous=function(l, n)
		local o = listmetadata[l]
		local n = n or 1
		return List.moveTo(l, o.position-n)
	end,

	reset=function(l)
		return List.moveTo(l, 0)
	end,
	
	current=function(l)
		local o = listmetadata[l]
		if not o.loaded then
			o.load(o.position, l)
		end
		return o.currentItem
	end,
	
	hasNext=function(l, n)
		local o = listmetadata[l]
		local n = n or 1
		return o.position+n-1 < o.count
	end,
	
	hasPrevious=function(l, n)
		local o = listmetadata[l]
		local n = n or 1
		return o.position-n+1>1
	end
}


local function arrayLoad(idx, list)
	local items = list.items or {}
	return items[idx]
end

-- Loads a page loading all items individually with the given load function
local function loadPage(pageNo, list)
	local o = listmetadata[l]
	local page = {}
	local pageStart = o.pageSize * (pageNo-1)
	
	for i=1, o.pageSize  do
		table.insert(page, o.load(pageStart+i, list))
	end
	return page
end

local passoverFunction = function(...) return ... end

--- Creates a new list
-- Creates a new list and registers it on the list metadata catalog
-- this way, lists are clean collections of items

function create(options)
	local list = setmetatable({}, List)
	list.items = options.items or {unpack(options)}

	local o = {}
	o.position = options.position or 0
	o.pageSize = options.pageSize or 10
	o.pages = {}
	o.maxLoadedPages = options.maxLoadedPages or 3
	o.mode = options.mode or MODE.ITEM
	
	o.load = options.load or arrayLoad
	o.loadPage = options.loadPage or loadPage

	o.onCreate = options.onCreate or passoverFunction
	o.onMove = options.onMove or passoverFunction
	o.onLoad = options.onLoad or passoverFunction
	o.onPageLoad = options.onPageLoad or passoverFunction
	o.onDestroy = options.onDestroy or passoverFunction

	o.count = type(options.count)=='function'  and options.count(options) or options.count or #list.items

	o.loaded = false
	
	listmetadata[list] = o
	
	return list
end

if testing then

	l = create{
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

	l = create{
		count=#t,
		mode=MODE.PAGE,
		load=function(idx)  return t[idx] end,
		onPageLoad=function() print'Page loaded' end
	}

	print(l.count, 'items')

	for o in l do
		print(o)
	end
end