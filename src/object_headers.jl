#
# Object headers
#

const HM_NIL = 0x00
const HM_DATASPACE = 0x01
const HM_LINK_INFO = 0x02
const HM_DATATYPE = 0x03
const HM_FILL_VALUE_OLD = 0x04
const HM_FILL_VALUE = 0x05
const HM_LINK_MESSAGE = 0x06
const HM_EXTERNAL_FILE_LIST = 0x07
const HM_DATA_LAYOUT = 0x08
const HM_BOGUS = 0x09
const HM_GROUP_INFO = 0x0a
const HM_FILTER_PIPELINE = 0x0b
const HM_ATTRIBUTE = 0x0c
const HM_OBJECT_COMMENT = 0x0d
const HM_SHARED_MESSAGE_TABLE = 0x0f
const HM_OBJECT_HEADER_CONTINUATION = 0x10
const HM_SYMBOL_TABLE = 0x11
const HM_MODIFICATION_TIME = 0x12
const HM_BTREE_K_VALUES = 0x13
const HM_DRIVER_INFO = 0x14
const HM_ATTRIBUTE_INFO = 0x15
const HM_REFERENCE_COUNT = 0x16

MESSAGE_TYPES = Dict(
    0x00 => "HM_NIL",
    0x01 => "HM_DATASPACE",
    0x02 => "HM_LINK_INFO",
    0x03 => "HM_DATATYPE",
    0x04 => "HM_FILL_VALUE_OLD",
    0x05 => "HM_FILL_VALUE",
    0x06 => "HM_LINK_MESSAGE",
    0x07 => "HM_EXTERNAL_FILE_LIST",
    0x08 => "HM_DATA_LAYOUT",
    0x09 => "HM_BOGUS",
    0x0a => "HM_GROUP_INFO",
    0x0b => "HM_FILTER_PIPELINE",
    0x0c => "HM_ATTRIBUTE",
    0x0d => "HM_OBJECT_COMMENT",
    0x0f => "HM_SHARED_MESSAGE_TABLE",
    0x10 => "HM_OBJECT_HEADER_CONTINUATION",
    0x11 => "HM_SYMBOL_TABLE",
    0x12 => "HM_MODIFICATION_TIME",
    0x13 => "HM_BTREE_K_VALUES",
    0x14 => "HM_DRIVER_INFO",
    0x15 => "HM_ATTRIBUTE_INFO",
    0x16 => "HM_REFERENCE_COUNT",
    )

const OH_ATTRIBUTE_CREATION_ORDER_TRACKED = 2^2
const OH_ATTRIBUTE_CREATION_ORDER_INDEXED = 2^3
const OH_ATTRIBUTE_PHASE_CHANGE_VALUES_STORED = 2^4
const OH_TIMES_STORED = 2^5

const OBJECT_HEADER_CONTINUATION_SIGNATURE = htol(0x4b48434f) # "OCHK"

struct ObjectStart
    signature::UInt32
    version::UInt8
    flags::UInt8
end
ObjectStart(flags::UInt8) = ObjectStart(OBJECT_HEADER_SIGNATURE, 2, flags)
define_packed(ObjectStart)

# Reads the start of an object including the signature, version, flags,
# and (payload) size. Returns the size.
# function read_obj_start(io::IO)
#     os = jlread(io, ObjectStart)
#     os.signature == OBJECT_HEADER_SIGNATURE || throw(InvalidDataException())
#     os.version == 2 || throw(UnsupportedVersionException())

#     if (os.flags & OH_TIMES_STORED) != 0
#         # Skip access, modification, change and birth times
#         skip(io, 128)
#     end
#     if (os.flags & OH_ATTRIBUTE_PHASE_CHANGE_VALUES_STORED) != 0
#         # Skip maximum # of attributes fields
#         skip(io, 32)
#     end

#     read_size(io, os.flags)
# end

function read_obj_start(io::IO)
    curpos = position(io)
    os = jlread(io, ObjectStart)
    if os.version == 2 && os.signature == OBJECT_HEADER_SIGNATURE
        if (os.flags & OH_TIMES_STORED) != 0
            # Skip access, modification, change and birth times
            skip(io, 128)
        end
        if (os.flags & OH_ATTRIBUTE_PHASE_CHANGE_VALUES_STORED) != 0
            # Skip maximum # of attributes fields
            skip(io, 32)
        end

        return read_size(io, os.flags), 2
    else
        seek(io, curpos)
        version = jlread(io, UInt8)
        version == 1 || throw(error("This should not have happened"))
        
        jlread(io, UInt8)
        num_messages = jlread(io, UInt16)
        #println("Reading $num_messages Messages")
        obj_ref_count = jlread(io, UInt32)
        #println("obj_ref_count = $obj_ref_count")
        obj_header_size = jlread(io, UInt32)
        #println("obj_header_size = $obj_header_size")
        #@info "If this errors, may need to skip to next 8bit alignment" maxlog = 1
        return obj_header_size, 1
    end
end

struct HeaderMessage
    msg_type::UInt8
    size::UInt16
    flags::UInt8
end
define_packed(HeaderMessage)


function isgroup(f::JLDFile, roffset::RelOffset)
    io = f.io
    seek(io, fileoffset(f, roffset))

    sz, version = read_obj_start(io)
    pmax::Int64 = position(io) + sz
    if version == 2
        while position(io) <= pmax-4
            msg = jlread(io, HeaderMessage)
            endpos = position(io) + msg.size
            if msg.msg_type == HM_LINK_INFO || msg.msg_type == HM_GROUP_INFO || msg.msg_type == HM_LINK_MESSAGE
                return true
            elseif msg.msg_type == HM_DATASPACE || msg.msg_type == HM_DATATYPE || msg.msg_type == HM_FILL_VALUE || msg.msg_type == HM_DATA_LAYOUT
                return false
            end
            seek(io, endpos)
        end
    elseif version == 1
        continuation_offset = -1
        continuation_length = 0
        chunk_end = pmax
        curpos = position(io)
        seek(io, curpos + 8 - mod1(curpos, 8))
        while true
            if continuation_offset != -1
                seek(io, continuation_offset)
                chunk_end = continuation_offset + continuation_length
                continuation_offset = -1
            end
            while position(io) <= chunk_end
                curpos = position(io)
                msg_type = jlread(io, UInt16)
                msg_size = jlread(io, UInt16)
                msg_flags = jlread(io, UInt8)
                #println("Message Type: $msg_type $(MESSAGE_TYPES[msg_type])")
                #println("Message Size: $msg_size")
                #println("Message flags: $msg_flags")
                jlread(io, UInt8); jlread(io, UInt16)
                msg = (;msg_type, size=msg_size, flags=msg_flags)
                endpos = position(io) + msg.size
                endpos = endpos + 8 - mod1(endpos, 8)
                if msg.msg_type == HM_LINK_INFO || msg.msg_type == HM_GROUP_INFO || msg.msg_type == HM_LINK_MESSAGE
                    return true
                elseif msg.msg_type == HM_DATASPACE || msg.msg_type == HM_DATATYPE || msg.msg_type == HM_FILL_VALUE || msg.msg_type == HM_DATA_LAYOUT
                    return false
                elseif msg_type == HM_OBJECT_HEADER_CONTINUATION
                    continuation_offset = chunk_start_offset = fileoffset(f, jlread(io, RelOffset))
                    continuation_length = jlread(io, Length)
                    #println("Next chunk at $continuation_offset with length=$continuation_length")
                end
                seek(io, endpos)
            end
            continuation_offset == -1 && break
        end
    end
    return false
end



function print_header_messages(f::JLDFile, roffset::RelOffset)
    io = f.io
    chunk_start_offset::Int64 = fileoffset(f, roffset)
    seek(io, chunk_start_offset)

    # Test for V1 Obj header
    version = jlread(io, UInt8)
    if version == 1
        println("Object Header Message Version 1")
        cio = io
        jlread(cio, UInt8)
        num_messages = jlread(cio, UInt16)
        obj_ref_count = jlread(cio, UInt32)
        obj_header_size = jlread(cio, UInt32)
        println("""
        Reading $num_messages Messages
            obj_ref_count = $obj_ref_count
            obj_header_size = $obj_header_size""")

        chunk_end = position(cio) + obj_header_size
        # Skip to nearest 8byte aligned position
        curpos = position(cio)
        skippos = curpos + 8 - mod1(curpos, 8)
        seek(cio, skippos)
    elseif version == 2
        println("Object Header Message Version 1")

        seek(io, chunk_start_offset)
        cio = begin_checksum_read(io)
        sz = read_obj_start(cio)
        chunk_end = position(cio) + sz
    else
        throw(UnsupportedVersionException("Unknown object header version $version"))
    end
    # Messages
    continuation_message_goes_here::Int64 = -1
    links = OrderedDict{String,RelOffset}()

    continuation_offset::Int64 = -1
    continuation_length::Length = 0
    next_link_offset::Int64 = -1
    link_phase_change_max_compact::Int64 = -1 
    link_phase_change_min_dense::Int64 = -1
    est_num_entries::Int64 = 4
    est_link_name_len::Int64 = 8
    chunk_end::Int64

    while true
        if continuation_offset != -1
            seek(io, continuation_offset)
            chunk_end = continuation_offset + continuation_length
            continuation_offset = -1
            if version == 2
                chunk_end = chunk_end #- 4 # leave space for checksum
                cio = begin_checksum_read(io)
                jlread(cio, UInt32) == OBJECT_HEADER_CONTINUATION_SIGNATURE || throw(InvalidDataException())
            end
        end

        while (curpos = position(cio)) <= chunk_end-4
            if version == 1
                msg_type = jlread(io, UInt16)
                msg_size = jlread(io, UInt16)
                flags = jlread(io, UInt8)
                jlread(io, UInt8); jlread(io, UInt16)
                endpos = curpos + 8 + msg_size
                endpos = endpos + 8 - mod1(endpos, 8)
                msg = (; msg_type, size=msg_size, flags)
            else # version == 2
                msg = jlread(cio, HeaderMessage)
                endpos = curpos + jlsizeof(HeaderMessage) + msg.size
            end
            println("""
            Header Message:  $(MESSAGE_TYPES[msg.msg_type])
                size: $(msg.size)
                flags: $(msg.flags)""")
            if msg.msg_type == HM_NIL
                if continuation_message_goes_here == -1 && 
                    chunk_end - curpos == CONTINUATION_MSG_SIZE
                    continuation_message_goes_here = curpos
                elseif endpos + CONTINUATION_MSG_SIZE == chunk_end
                    # This is the remaining space at the end of a chunk
                    # Use only if a message can potentially fit inside
                    # Single Character Name Link Message has 13 bytes payload
                    if msg.size >= 13 
                        next_link_offset = curpos
                    end
                end
            else
                continuation_message_goes_here = -1
                if msg.msg_type == HM_LINK_INFO
                    link_info = jlread(cio, LinkInfo)
                    link_info.fractal_heap_address == UNDEFINED_ADDRESS || throw(UnsupportedFeatureException())
                elseif msg.msg_type == HM_GROUP_INFO
                    if msg.size > 2
                        # Version Flag
                        jlread(io, UInt8) == 0 || throw(UnsupportedFeatureException()) 
                        flag = jlread(io, UInt8)
                        if flag%2 == 1 # first bit set
                            link_phase_change_max_compact = jlread(io, UInt16)
                            link_phase_change_min_dense = jlread(io, UInt16)
                            println("    link_phase_change_max_compact = $link_phase_change_max_compact")
                            println("    link_phase_change_min_dense = $link_phase_change_min_dense")
                        end
                        if (flag >> 1)%2 == 1 # second bit set
                            # Verify that non-default group size is given
                            est_num_entries = jlread(io, UInt16)
                            est_link_name_len = jlread(io, UInt16)
                            println("    est_num_entries = $est_num_entries")
                            println("    est_link_name_len = $est_link_name_len")
                        end
                    end
                elseif msg.msg_type == HM_LINK_MESSAGE
                    name, loffset = read_link(cio)
                    links[name] = loffset
                    println("   name = \"$name\"")
                    println("   offset = $(Int(loffset.offset))")
                elseif msg.msg_type == HM_OBJECT_HEADER_CONTINUATION
                    continuation_offset = chunk_start_offset = fileoffset(f, jlread(cio, RelOffset))
                    continuation_length = jlread(cio, Length)
                    # For correct behaviour, empty space can only be filled in the 
                    # very last chunk. Forget about previously found empty space
                    next_link_offset = -1
                    println("""    offset = $(continuation_offset)\n    length = $(continuation_length)""")
                elseif msg.msg_type == HM_DATASPACE
                    dataspace = read_dataspace_message(cio)
                    println("    $dataspace")
                elseif msg.msg_type == HM_DATATYPE
                    datatype_class, datatype_offset = read_datatype_message(cio, f, (msg.flags & 2) == 2)
                    println("""    class: $class\n    offset: $datatype_offset""")
                elseif msg.msg_type == HM_FILL_VALUE_OLD
                    #(jlread(cio, UInt8) == 3 && jlread(cio, UInt8) == 0x09) || throw(UnsupportedFeatureException())
                elseif msg.msg_type == HM_FILL_VALUE
                    #(jlread(cio, UInt8) == 3 && jlread(cio, UInt8) == 0x09) || throw(UnsupportedFeatureException())
                elseif msg.msg_type == HM_DATA_LAYOUT
                    version = jlread(cio, UInt8)
                    println("""    version: $version""")
                    if version == 4 || version == 3
                        storage_type = jlread(cio, UInt8)
                        if storage_type == LC_COMPACT_STORAGE
                            data_length = jlread(cio, UInt16)
                            data_offset = position(cio)
                            println("""    type: compact storage\n    length: $length\n    offset: $(data_offset)""")
                        elseif storage_type == LC_CONTIGUOUS_STORAGE
                            data_offset = fileoffset(f, jlread(cio, RelOffset))
                            data_length = jlread(cio, Length)
                            println("""    type: contiguous storage\n    length: $length\n    offset: $(data_offset)""")

                        elseif storage_type == LC_CHUNKED_STORAGE
                            # TODO: validate this
                            flags = jlread(cio, UInt8)
                            dimensionality = jlread(cio, UInt8)
                            dimensionality_size = jlread(cio, UInt8)
                            skip(cio, Int(dimensionality)*Int(dimensionality_size))
        
                            chunk_indexing_type = jlread(cio, UInt8)
                            chunk_indexing_type == 1 || throw(UnsupportedFeatureException("Unknown chunk indexing type"))
                            data_length = jlread(cio, Length)
                            jlread(cio, UInt32)
                            data_offset = fileoffset(f, jlread(cio, RelOffset))
                            chunked_storage = true
                            println("""    type: chunked storage
                                    length: $length
                                    offset: $(data_offset)
                                    dimensionality: $dimensionality
                                    dimensionality_size: $dimensionality_size
                                    chunk indexing type: $chunk_indexing_type""")

                        else
                            throw(UnsupportedFeatureException("Unknown data layout"))
                        end
                    end
                elseif msg.msg_type == HM_FILTER_PIPELINE
                    version = jlread(cio, UInt8)
                    version == 2 || throw(UnsupportedVersionException("Filter Pipeline Message version $version is not implemented"))
                    nfilters = jlread(cio, UInt8)
                    nfilters == 1 || throw(UnsupportedFeatureException())
                    filter_id = jlread(cio, UInt16)
                    issupported_filter(filter_id) || throw(UnsupportedFeatureException("Unknown Compression Filter $filter_id"))
                elseif msg.msg_type == HM_ATTRIBUTE
                    if attrs === EMPTY_READ_ATTRIBUTES
                        attrs = ReadAttribute[read_attribute(cio, f)]
                    else
                        push!(attrs, read_attribute(cio, f))
                    end
                elseif (msg.flags & 2^3) != 0
                    throw(UnsupportedFeatureException())
                end
            end
            seek(cio, endpos)
        end

        # Checksum
        seek(cio, chunk_end)
        if version == 2
            end_checksum(cio) == jlread(io, UInt32) || throw(InvalidDataException())
        end
        continuation_offset == -1 && break
    end
    nothing
end