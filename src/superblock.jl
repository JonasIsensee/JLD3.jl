#
# Superblock
#

const SUPERBLOCK_SIGNATURE = htol(0x0a1a0a0d46444889) # UInt8[0o211, 'H', 'D', 'F', '\r', '\n', 0o032, '\n']

# https://www.hdfgroup.org/HDF5/doc/H5.format.html#FileMetaData
# Superblock (Version 2)
# struct Superblock
#     file_consistency_flags::UInt8
#     base_address::Int64
#     superblock_extension_address::RelOffset
#     end_of_file_address::Int64
#     root_group_object_header_address::RelOffset
# end

struct Superblock
    version::UInt8
    base_address::UInt64
    size_of_offsets::UInt8
    size_of_lengths::UInt8
    end_of_file_address::UInt64
    data
end

jlsizeof(::Union{Type{Superblock},Superblock}) =
    12+jlsizeof(RelOffset)*4+4



function jlread(io::IO, ::Type{Superblock})
    cio = begin_checksum_read(io)

    # Signature
    signature = jlread(cio, UInt64)
    signature == SUPERBLOCK_SIGNATURE || throw(InvalidDataException())

    # Version
    version = jlread(cio, UInt8)
    if version == 0
        version_free_space_storage = jlread(cio, UInt8) # has to be zero
        version_root_group_symbol_table_enty = jlread(cio, UInt8) # has to be zero
        jlread(cio, UInt8)
        version_share_header_msg_format = jlread(cio, UInt8) # has to be zero
        size_of_offsets = jlread(cio, UInt8)
        OffsetType = uintofsize(size_of_offsets)
        size_of_lengths = jlread(cio, UInt8)
        jlread(cio, UInt8)
        group_leaf_node_k = jlread(cio, UInt16) # must be greater than zero
        group_internal_node_k = jlread(cio, UInt16) # must be greater than zero
        # Unused File consistency flags
        jlread(cio, UInt32)
        #indexed_storage_internal_node_k = jlread(cio, UInt16) # must be greater than zero
        #jlread(cio, UInt16)
        base_address = jlread(cio, OffsetType) # base adress for offsets within file (also absolute address of superblock)
        adress_free_space_info = jlread(cio, OffsetType)  # Undefined Adress
        end_of_file_address = jlread(cio, OffsetType) # absolute adress of first byte past end of data
        driver_info_block_adress = jlread(cio, OffsetType) # undefined of relative adress of driver info block
        #root_group_symbol_table_entry = jlread(cio, UInt32) # symbol table entry of root group

        link_name_offset = jlread(cio, OffsetType)
        obj_header_adress = jlread(cio, OffsetType)
        cachetype = jlread(cio, UInt32)
        reserved = jlread(cio, UInt32)
        scratchspace = jlread(cio, UInt128)

        # Discard Checksum
        end_checksum(cio)

        Superblock(0, base_address, size_of_offsets, size_of_lengths, end_of_file_address,
                    (;#version_free_space_storage,
                    #version_root_group_symbol_table_enty,
                    #version_share_header_msg_format,
                    group_leaf_node_k,
                    group_internal_node_k,
                    #indexed_storage_internal_node_k,
                    #adress_free_space_info,
                    driver_info_block_adress,
                    #root_group_symbol_table_entry
                    link_name_offset,
                    obj_header_adress,
                    cachetype,
                       ))
    elseif version == 2 || version == 3
        
        # Size of offsets and size of lengths
        size_of_offsets = jlread(cio, UInt8)
        size_of_lengths = jlread(cio, UInt8)
        (size_of_offsets == 8 && size_of_lengths == 8) || throw(UnsupportedFeatureException())

        # File consistency flags
        file_consistency_flags = jlread(cio, UInt8)

        # Addresses
        base_address = jlread(cio, Int64)
        superblock_extension_address = jlread(cio, RelOffset)
        end_of_file_address = jlread(cio, Int64)
        root_group_object_header_address = jlread(cio, RelOffset)

        # Checksum
        cs = end_checksum(cio)
        jlread(io, UInt32) == cs || throw(InvalidDataException())

        Superblock(version, base_address, size_of_offsets, size_of_lengths, end_of_file_address,
                    (;
                    superblock_extension_address,
                    root_group_object_header_address))
    else
        throw(UnsupportedVersionException("superblock version $version is not supported."))
    end
end

function jlwrite(io::IO, s::Superblock)
    cio = begin_checksum_write(io, 8+4+4*jlsizeof(RelOffset))
    jlwrite(cio, SUPERBLOCK_SIGNATURE::UInt64)    # Signature
    jlwrite(cio, UInt8(2))                        # Version
    jlwrite(cio, UInt8(8))                        # Size of offsets
    jlwrite(cio, UInt8(8))                        # Size of lengths
    jlwrite(cio, s.file_consistency_flags::UInt8)
    jlwrite(cio, s.base_address::Int64)
    jlwrite(cio, s.superblock_extension_address::RelOffset)
    jlwrite(cio, s.end_of_file_address::Int64)
    jlwrite(cio, s.root_group_object_header_address::RelOffset)
    jlwrite(io, end_checksum(cio))
end

function find_superblock(f)#::JLDFile)
    # Search at 0, 512, 1024, 2048 ...
    for offset in (0, 512, 1024, 2048, 4096)
        seek(f.io, offset)
        # Signature
        signature = jlread(f.io, UInt64)
        if signature == SUPERBLOCK_SIGNATURE
            @info "Found Superblock at $(offset)"
            seek(f.io, offset)
            return jlread(f.io, Superblock)
        end
    end
    throw(InvalidDataException("Did not find a Superblock"))
end
