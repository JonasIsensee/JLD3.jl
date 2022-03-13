

# function load_btree_group(f, offset)
#     io = f.io
#     seek(io, Int(offset))
#     # # Symbol Table Node
#     # signature = htol(0x444f4e53) # UInt8['S', 'N', 'O', 'D']
#     # sig =  jlread(io, UInt32) 
#     # # signature
#     # println(sig, "==", signature)
#     load_obj_header(io)
# end

# struct HeaderMessage_v1
#     msg_type::UInt16
#     size::UInt16
#     flags::UInt8
# end
# define_packed(HeaderMessage_v1)

function load_v1_group(f, roffset)
    io = f.io
    chunk_start_offset::Int64 = fileoffset(f, roffset)
    seek(io, chunk_start_offset)

    version = jlread(io, UInt8)
    version == 1 || throw(error("This should not have happened"))
    
    jlread(io, UInt8)
    num_messages = jlread(io, UInt16)
    #println("Reading $num_messages Messages")
    obj_ref_count = jlread(io, UInt32)
    #println("obj_ref_count = $obj_ref_count")
    obj_header_size = jlread(io, UInt32)
    #println("obj_header_size = $obj_header_size")

    chunk_end::Int64 = position(io) + obj_header_size

    # Skip to nearest 8byte aligned position
    curpos = position(io)
    skippos = curpos + 8 - mod1(curpos, 8)
    seek(io, skippos)

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

    while true
        if continuation_offset != -1
            seek(io, continuation_offset)
            chunk_end = continuation_offset + continuation_length
            continuation_offset = -1

            #jlread(io, UInt32) == OBJECT_HEADER_CONTINUATION_SIGNATURE || throw(InvalidDataException())
            # No special signature for V1 Object headers
        end

        while (curpos = position(io)) < chunk_end
            msg_type = jlread(io, UInt16)
            #println("Message Type: $msg_type $(MESSAGE_TYPES[msg_type])")
            msg_size = jlread(io, UInt16)
            #println("Message Size: $msg_size")
            msg_flags = jlread(io, UInt8)
            #println("Message flags: $msg_flags")
            jlread(io, UInt8); jlread(io, UInt16)
            endpos = curpos + 8 + msg_size
            endpos = endpos + 8 - mod1(endpos, 8)
            if msg_type == HM_NIL
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
                if msg_type == HM_LINK_INFO
                    link_info = jlread(io, LinkInfo)
                    link_info.fractal_heap_address == UNDEFINED_ADDRESS || throw(UnsupportedFeatureException())
                elseif msg_type == HM_GROUP_INFO
                    if msg_size > 2
                        # Version Flag
                        jlread(io, UInt8) == 0 || throw(UnsupportedFeatureException()) 
                        flag = jlread(io, UInt8)
                        if flag%2 == 1 # first bit set
                            link_phase_change_max_compact = jlread(io, UInt16)
                            link_phase_change_min_dense = jlread(io, UInt16)
                        end
                        if (flag >> 1)%2 == 1 # second bit set
                            # Verify that non-default group size is given
                            est_num_entries = jlread(io, UInt16)
                            est_link_name_len = jlread(io, UInt16)
                        end
                    end
                elseif msg_type == HM_LINK_MESSAGE
                    name, loffset = read_link(io)
                    links[name] = loffset
                elseif msg_type == HM_OBJECT_HEADER_CONTINUATION
                    continuation_offset = chunk_start_offset = fileoffset(f, jlread(io, RelOffset))
                    continuation_length = jlread(io, Length)
                    println("Next chunk at $continuation_offset with length=$continuation_length")
                    # For correct behaviour, empty space can only be filled in the 
                    # very last chunk. Forget about previously found empty space
                    next_link_offset = -1
                elseif (msg_flags & 2^3) != 0
                    throw(UnsupportedFeatureException())
                end
            end
            seek(io, endpos)
        end
        continuation_offset == -1 && break
    end
    return Group{typeof(f)}(f, chunk_start_offset, continuation_message_goes_here,        
                     chunk_end, next_link_offset, est_num_entries,
                     est_link_name_len,
                     OrderedDict{String,RelOffset}(), OrderedDict{String,Group}(), links)
end