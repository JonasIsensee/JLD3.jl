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
                #println("Message Type: $msg_type $(MESSAGE_TYPES[msg_type])")
                msg_size = jlread(io, UInt16)
                #println("Message Size: $msg_size")
                msg_flags = jlread(io, UInt8)
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
