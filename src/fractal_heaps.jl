const FRACTAL_HEAP_HEADER_SIGNATURE = htol(0x50485246) # UInt8['F','R','H','P']
const FRACTAL_HEAP_INDIRECT_BLOCK_SIGNATURE = htol(0x42494846) # UInt8['F','H','I','B']
const FRACTAL_HEAP_DIRECT_BLOCK_SIGNATURE = htol(0x42444846) # UInt8['F', 'H', 'D', 'B']

struct FractalHeapHeader
    offset::RelOffset
    table_width::Int
    starting_block_size::Int
    max_direct_block_size::Int
    max_heap_size::Int
    root_block_address::RelOffset
    cur_num_rows_in_root_iblock::Int
    has_io_filter::Bool
    max_dblock_rows::Int
    max_size_managed_objects::Int
    # could add the rest of the fields if they ever become necessary
end

struct FractalHeapDirectBlock
    offset::RelOffset # position of block in file
    # block offset in heaps address space 
    # WARNING: don't use. sometimes wrong in long files
    block_offset::UInt64 
    size::UInt64
    filtered_size::UInt64 # set to typemax if not filtered
    filter_mask::UInt32  # set to typemax if not filtered
end

struct FractalHeapIndirectBlock
    offset::RelOffset # position of iblock in file
    block_offset::UInt64 # block offset in heaps address space
    dblocks::Vector{FractalHeapDirectBlock}
    iblocks::Vector{FractalHeapIndirectBlock}
end

function blocksize(blocknum, starting_size, table_width)
    #block numbering starts at zero
    rownum = Int(blocknum ÷ table_width)
    (2^(max(0,rownum-1))) * starting_size
end

function block_num_size_start(offset, hh)
    width = hh.table_width
    # first compute row number
    r = Int(offset ÷ (hh.starting_block_size*width))
    r > 2 && (r = ceil(Int, log2(r+1)))
    # row start offset
    row_startoffset = (r>1 ? 2^(r-1) : r)*hh.starting_block_size*width
    block_size = (2^(max(0,r-1))) * hh.starting_block_size
    block_num = width*r + (offset-row_startoffset) ÷ block_size
    block_start = row_startoffset + block_size*(block_num-width*r)
    block_num, block_size, block_start
end


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
    num_starting_rows_in_root_iblock = jlread(cio, UInt16)
    root_block_address = jlread(cio, RelOffset)#OffsetType) # RelOffset ?
    cur_num_rows_in_root_iblock = jlread(cio, UInt16)

    has_io_filter = io_filter_encoded_length > 0
    if has_io_filter
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

    max_dblock_rows = (log2(max_direct_block_size) - log2(starting_block_size))+2 |> Int

    FractalHeapHeader(offset, table_width, starting_block_size, max_direct_block_size, max_heap_size,
        root_block_address, cur_num_rows_in_root_iblock, has_io_filter, max_dblock_rows,
        max_size_managed_objects)
end

function read_indirect_block(f, offset, hh, nrows)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset))
    cio = begin_checksum_read(io)

    signature = jlread(cio, UInt32)
    signature == FRACTAL_HEAP_INDIRECT_BLOCK_SIGNATURE || throw(InvalidDataException("Signature does not match."))

    version = jlread(cio, UInt8)
    heap_header_address = jlread(cio, OffsetType)
    # number of bytes for block offset
    offset_byte_num = ceil(Int, hh.max_heap_size / 8)
    block_offset = to_uint64(jlread(cio, UInt8, offset_byte_num))

    # Read child direct blocks
    block_start = block_offset
    K = min(nrows, hh.max_dblock_rows)*hh.table_width
    dblocks = map(1:K) do k
        dblock_address = jlread(cio, RelOffset) # OffsetType
        dblock_size = blocksize(k-1, hh.starting_block_size, hh.table_width)
        if hh.has_io_filter > 0
            filtered_size = jlread(cio, LengthType)
            filter_mask = jlread(cio, UInt32)
        else
            filtered_size = typemax(UInt64)
            filter_mask = typemax(UInt32)
        end
        dblock = FractalHeapDirectBlock(dblock_address, block_start, dblock_size, filtered_size, filter_mask)
        block_start += dblock_size
        return dblock
    end
    N = (nrows <= hh.max_dblock_rows) ? 0 :  (nrows-hh.max_dblock_rows)*hh.table_width
    iblock_addresses = map(1:N) do n
        jlread(cio, RelOffset) #OffsetType
    end

    # Checksum
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException()) 
    
    iblocks = Vector{FractalHeapIndirectBlock}(undef, N)
    for n=1:N
        iblock_offset = iblock_addresses[n] 
        iblock_offset == UNDEFINED_ADDRESS && break
        # figure out iblock size / nrows
        block_num = K+(n-1)
        rownum = block_num ÷ hh.table_width
        block_size = (2^(max(0,rownum-1))) * hh.starting_block_size
        sub_iblock_nrows = (log2(block_size)-log2(hh.starting_block_size* hh.table_width))+1
        iblocks[n] = read_indirect_block(f, iblock_offset , hh, sub_iblock_nrows)
    end
    FractalHeapIndirectBlock(offset, block_offset, dblocks, iblocks)
end

#####################################################################################################
## Version 2 B-trees 
#####################################################################################################

const V2_BTREE_HEADER_SIGNATURE = htol(0x44485442) # UInt8['B','T','H','D']
const V2_BTREE_INTERNAL_NODE_SIGNATURE = htol(0x4e495442) # UInt8['B', 'T', 'I', 'N']
const V2_BTREE_LEAF_NODE_SIGNATURE = htol(0x464c5442) # UInt8['B', 'T', 'L', 'F']

struct BTreeHeaderV2
    offset::RelOffset
    type::Int
    node_size::Int
    record_size::Int
    depth::Int
    split_percent::Int
    merge_percent::Int
    root_node_address::RelOffset
    num_records_in_root_node::Int
    num_records_in_tree::Int
end

abstract type BTreeNodeV2 end
abstract type BTreeRecordV2 end

struct BTreeInternalNodeV2 <: BTreeNodeV2
    offset::RelOffset
    type::UInt8
    records::Vector{Any}
    child_nodes::Vector #abstract to defer loading
end

struct BTreeLeafNodeV2 <: BTreeNodeV2
    offset::RelOffset
    type::UInt8
    records::Vector{<:BTreeRecordV2}
end

struct BTreeType5RecordV2 <: BTreeRecordV2
    hash::UInt32
    offset::UInt64
    length::Int
end

function read_v2btree_header(f, offset)
    OffsetType = uintofsize(f.superblock.size_of_offsets)
    LengthType = uintofsize(f.superblock.size_of_lengths)

    io = f.io
    seek(io, fileoffset(f, offset))
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
    BTreeHeaderV2(  offset,
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
    max_records_total = 0
    numbytes = size_size(max_records)
    numbytes_total = 0

    for d = 1:depth
        space = bh.node_size - 4-1-1-4 - sizeof(RelOffset) - (d>1)*numbytes_total
        max_records = space ÷ (bh.record_size + sizeof(RelOffset) + numbytes+(d>1)*numbytes_total)
        numbytes = size_size(max_records)
        max_records_total = max_records + (max_records+1)*max_records_total
        numbytes_total = size_size(max_records_total)
    end
    numbytes_total = size_size2(max_records_total)
    child_nodes = map(1:num_records+1) do _
        child_node_pointer = jlread(cio, RelOffset) # offset type
        num_records = Int(to_uint64(jlread(cio, UInt8, numbytes)))
        if depth > 1
            total_records = Int(to_uint64(jlread(cio, UInt8, numbytes_total)))
            return (; child_node_pointer, num_records,total_records)
        end
        (; child_node_pointer, num_records)
    end
    end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())    
    
    BTreeInternalNodeV2(offset, type, records, child_nodes)
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
    BTreeLeafNodeV2(offset, type, records)
end



function read_record(io, type, hh)
    if type == 5 # link name for indexed group
        hash_of_name = jlread(io, UInt32)
        # read Heap id for managed object
        version_type = jlread(io, UInt8)

        offbytes = hh.max_heap_size÷8
        offset =Int(to_uint64(jlread(io, UInt8, offbytes)))
        lnbytes = min(hh.max_direct_block_size, hh.max_size_managed_objects) |> size_size2
        length = Int(to_uint64(jlread(io, UInt8, lnbytes)))
        skip(io, 6-offbytes-lnbytes)
        return BTreeType5RecordV2(hash_of_name, offset, length)
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

function get_block_offset(f, iblock, roffset, hh)
    block_num, block_size, block_start = block_num_size_start(roffset, hh)
    K = length(iblock.dblocks)
    if block_num < K
        dblock = iblock.dblocks[block_num+1]
        return fileoffset(f,dblock.offset) + roffset - block_start
    end
    sub_iblock =  iblock.iblocks[block_num-K+1]
    get_block_offset(f, sub_iblock, roffset-block_start, hh)
end

function read_btree(f, offset_hh, offset_bh)
    hh = read_fractal_heap_header(f, offset_hh)
    bh = read_v2btree_header(f, offset_bh)
    
    records = read_records_in_node(f, bh.root_node_address, bh.num_records_in_root_node, bh.depth, bh, hh)
    indirect_rb = read_indirect_block(f, hh.root_block_address, hh, hh.cur_num_rows_in_root_iblock)
    links = map(records) do r
        offset = get_block_offset(f, indirect_rb, r.offset, hh)
        seek(f.io, offset)
        read_link(f.io)
    end
    links
end


