const FRACTAL_HEAP_HEADER_SIGNATURE = htol(0x50485246) # UInt8['F','R','H','P']
const FRACTAL_HEAP_INDIRECT_BLOCK_SIGNATURE = htol(0x42494846) # UInt8['F','H','I','B']
const FRACTAL_HEAP_DIRECT_BLOCK_SIGNATURE = htol(0x42444846) # UInt8['F', 'H', 'D', 'B']

function read_fractal_heap_header(f, offset)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset)) # may need to compute fileoffset
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == FRACTAL_HEAP_HEADER_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    heap_id_length = jlread(cio, UInt16)
    io_filter_encoded_length = jlread(cio, UInt16)
    flags = jlread(cio, UInt8)
    max_size_managed_objects = jlread(cio, UInt32)
    next_huge_object_id = jlread(cio, LengthType)
    huge_object_v2btree_address = jlread(cio, OffsetType)
    free_space_in_managed_blocks = jlread(cio, LengthType)
    managed_block_free_space_manager = jlread(cio, OffsetType)
    managed_space_in_heap = jlread(cio, LengthType)
    allocated_space_in_heap = jlread(cio, LengthType)
    direct_block_allocation_iterator_offset = jlread(cio, LengthType)
    managed_objects_number_in_heap = jlread(cio, LengthType)
    huge_objects_size_in_heap = jlread(cio, LengthType)
    huge_objects_number_in_heap = jlread(cio, LengthType)
    tiny_objects_size_in_heap = jlread(cio, LengthType)
    tiny_objects_number_in_heap = jlread(cio, LengthType)

    table_width = jlread(cio, UInt16)
    starting_block_size = jlread(cio, LengthType)
    max_direct_block_size = jlread(cio, LengthType)
    max_heap_size = jlread(cio, UInt16)
    num_starting_rows_in_root_indirect_block = jlread(cio, UInt16)
    root_block_address = jlread(cio, RelOffset)#OffsetType) # RelOffset ?
    cur_num_rows_in_root_indirect_block = jlread(cio, UInt16)
    if io_filter_encoded_length > 0
        filtered_root_direct_block_size = jlread(cio, LengthType)
        io_filter_mask = jlread(cio, UInt32)
        io_filter_information = jlread(cio, UInt8, io_filter_encoded_length)
    else
        filtered_root_direct_block_size = typemax(LengthType)
        io_filter_mask = typemax(UInt32)
        io_filter_information = UInt8[]
    end

    # Checksum
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException("Invalid Checksum"))

    # 
    max_dblock_rows = (log2(max_direct_block_size) - log2(starting_block_size))+2 |> Int
    
    (; version, heap_id_length, io_filter_encoded_length, flags, max_size_managed_objects, next_huge_object_id,
        huge_object_v2btree_address, free_space_in_managed_blocks, managed_block_free_space_manager, 
        managed_space_in_heap, allocated_space_in_heap, direct_block_allocation_iterator_offset, managed_objects_number_in_heap,
        huge_objects_number_in_heap, huge_objects_size_in_heap, tiny_objects_size_in_heap, tiny_objects_number_in_heap,
        table_width, starting_block_size,max_direct_block_size, max_heap_size, num_starting_rows_in_root_indirect_block,
        root_block_address, cur_num_rows_in_root_indirect_block, filtered_root_direct_block_size,
        io_filter_mask, io_filter_information, max_dblock_rows)
end

function decode_fractal_heap(f, offset)
    hh = read_fractal_heap_header(f, offset)
    if hh.cur_num_rows_in_root_indirect_block == 0
        #read direct block at root block Adress
    else
        #read indirect block at root block address
    end
end

function read_indirect_block(f, offset, hh)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset)) # may need to compute fileoffset
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == FRACTAL_HEAP_INDIRECT_BLOCK_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    heap_header_address = jlread(cio, OffsetType)
    # number of bytes for block offset
    offset_byte_num = ceil(Int, hh.max_heap_size / 8)
    block_offset = jlread(cio, UInt8, offset_byte_num)

    # Number of rows in root indirect block 
    # I think this is simpler in the root indirect block
    nrows = hh.cur_num_rows_in_root_indirect_block
    K = min(nrows, hh.max_dblock_rows)*hh.table_width
    direct_blocks = map(1:K) do k
        child_direct_block_address = jlread(cio, RelOffset) # OffsetType
        # How large are these blocks ?
        # k=1 → starting_block_size
        # k=2 → starting_block_size
        # k=3 → 2x starting_block_size
        # k=4 → 4x starting_block_size
        if hh.io_filter_encoded_length > 0 # some filtering active
            filtered_direct_block_size = jlread(cio, LengthType)
            filter_mask_for_direct_block = jlread(cio, UInt32)
            return (;child_direct_block_address, filtered_direct_block_size, filter_mask_for_direct_block)
        end
        (;child_direct_block_address)
    end
    N = nrows <= hh.max_dblock_rows ? 0 : K - hh.max_dblock_rows*hh.table_width
    indirect_blocks = map(1:N) do n
        child_indirect_block_address = jlread(cio, OffsetType)
    end

    # Checksum
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())    
    (; version, heap_header_address, block_offset, nrows, K, direct_blocks, N, indirect_blocks)
end


function read_direct_block(f, offset, hh, block_size)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset))
    # checksummed if flag is set in header
    if (hh.flags & 0x2) == 0x2
        cio = begin_checksum_read(io)
    else
        cio = io
    end
    signature = jlread(cio, UInt32)
    signature == FRACTAL_HEAP_DIRECT_BLOCK_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    println(version)
    heap_header_address = jlread(cio, OffsetType)
    println(heap_header_address)
    # number of bytes for block offset
    offset_byte_num = ceil(Int, hh.max_heap_size / 8) +0
    println(offset_byte_num)
    block_offset = [jlread(cio, UInt8) for _=1:offset_byte_num]
    println(block_offset)
    
    if (hh.flags & 0x2) == 0x2
        # Checksum
        println("Checksum: $(end_checksum(cio)) == $(jlread(io, UInt32))")# || throw(InvalidDataException())    
        #println("Checksum: $(end_checksum(cio)) == $(jlread(io, UInt32))")# || throw(InvalidDataException())    
    end
    #println("end checksum $(end_checksum(cio))")
    #println(jlread(io, UInt32)) # checksum?)
    #data_size = fileoffset(f, offset)+block_size - position(io)
    #println(jlread(io,UInt8, data_size))
    # read object
    #id = jlread(io, UInt8)
    #println(id)
    # Find out if tiny
    #id >> 6 == 0
end


#####################################################################################################
## Version 2 B-trees 
const V2_BTREE_HEADER_SIGNATURE = htol(0x44485442) # UInt8['B','T','H','D']
const V2_BTREE_INTERNAL_NODE_SIGNATURE = htol(0x4e495442) # UInt8['B', 'T', 'I', 'N']
const V2_BTREE_LEAF_NODE_SIGNATURE = htol(0x464c5442) # UInt8['B', 'T', 'L', 'F']


function read_v2btree_header(f, offset)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset)) # may need to compute fileoffset
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == V2_BTREE_HEADER_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    type = jlread(cio, UInt8)
    node_size = jlread(cio, UInt32)
    record_size = jlread(cio, UInt16)
    depth = jlread(cio, UInt16)
    split_percent = jlread(cio, UInt8)
    merge_percent = jlread(cio, UInt8)
    root_node_address = jlread(cio, RelOffset)#OffsetType)
    num_records_in_root_node = jlread(cio, UInt16)
    num_records_in_tree = jlread(cio, LengthType)

    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())    
    (; version,
        type,
        node_size,
        record_size,
        depth,
        split_percent,
        merge_percent,
        root_node_address,
        num_records_in_root_node,
        num_records_in_tree)
end

function read_v2btree_node(f, offset, num_records, depth, bh, hh)
    if depth == 0
        return read_v2btree_leaf_node(f, offset, num_records, bh, hh)
    end
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset)) # may need to compute fileoffset
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == V2_BTREE_INTERNAL_NODE_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    type = jlread(cio, UInt8)

    records = map(1:num_records) do n 
        read_record(cio, type, hh)
    end

    # determine number of bytes used to encode `num_records`
    # this has to be done iteratively
    # leaf node:
    space = bh.node_size - 4 - 1 - 1 - 4
    max_records = space ÷ bh.record_size
    numbytes = size_size(max_records)
    #println("Leaf Node ")
    for d = 1:depth
        space = bh.node_size - 4-1-1-4 - sizeof(RelOffset) - numbytes*(1+(d>1))
        max_records = space ÷ (bh.record_size + sizeof(RelOffset) + numbytes*(1+(d>1)))
        numbytes = size_size(max_records)
    end

    
    child_nodes = map(1:num_records+1) do _
        child_node_pointer = jlread(cio, RelOffset) # offset type
        num_records = to_uint64(jlread(cio, UInt8, numbytes))
        (; child_node_pointer, num_records)
    end
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())    
    (; version, type, records, child_nodes)
end


function read_v2btree_leaf_node(f, offset, num_records, bh, hh)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset)) # may need to compute fileoffset
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == V2_BTREE_LEAF_NODE_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    type = jlread(cio, UInt8)

    records = map(1:num_records) do n 
        read_record(cio, type, hh)
    end
  
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())    
    (; version, type, records)
end

function read_record(io, type, hh)
    if type == 5 # link name for indexed group
        hash_of_name = jlread(io, UInt32)
        # read Heap id for managed object
        version_type = jlread(io, UInt8)

        offbytes = hh.max_heap_size÷8
        offset =Int(to_uint64(jlread(io, UInt8, offbytes)))
        #jlread(io, UInt8)
        lnbytes = min(hh.max_direct_block_size, hh.max_size_managed_objects) |> size_size2
        length = Int(to_uint64(jlread(io, UInt8, lnbytes)))
        #println("offset=$offset lnbytes=$lnbytes length=$length")
        jlread(io, UInt8, 6-offbytes-lnbytes)
        #id = jlread(io, UInt8, 7)
        #id2 = Int(to_uint64([id; 0x0]))

        return (;hash_of_name, offset, length)
    else
        throw(error("Not implemented record type"))
    end
end

function read_records_in_node(f, offset, num_records, depth, bh, hh)
    if depth == 0
        return read_v2btree_leaf_node(f, offset, num_records, bh, hh).records
    end
    
    node = read_v2btree_node(f, offset, num_records, depth, bh, hh)

    records = []
    for n=1:num_records+1
        child_offset = node.child_nodes[n].child_node_pointer
        child_records = node.child_nodes[n].num_records
        records_in_child = read_records_in_node(f, child_offset, child_records, depth-1, bh, hh)
        append!(records, records_in_child)
        n<=num_records && (push!(records, node.records[n]))
    end
    return records
end

function block_num(offset, hh)
    b = (offset ÷ hh.starting_block_size)
    b <= 2 && return b
    ceil(Int, log2(b+1))
end

function read_btree(f, offset_hh, offset_bh)
    hh = read_fractal_heap_header(f, offset_hh)
    bh = read_v2btree_header(f, offset_bh)
    
    records = read_records_in_node(f, bh.root_node_address, bh.num_records_in_root_node, bh.depth, bh, hh)
    indirect_rb = read_indirect_block(f, hh.root_block_address, hh)

    map(records) do r
        bn = block_num(r.offset, hh)
        startoffset = (bn>2 ? 2^(bn-1) : bn)*hh.starting_block_size
        block_offset= indirect_rb.direct_blocks[bn+1].child_direct_block_address
        seek(f.io, fileoffset(f,block_offset)+r.offset-startoffset)
        read_link(f.io)
    end
end


