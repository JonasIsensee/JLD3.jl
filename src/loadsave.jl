function jldopen(@nospecialize(f::Function), args...; kws...)
    jld = jldopen(args...; kws...)
    try
        return f(jld)
    finally
        close(jld)
    end
end


function loadtodict!(d::Dict, g::Union{JLDFile,Group}, prefix::String="")
    for k in keys(g)
        v = g[k]
        if v isa Group
            loadtodict!(d, v, prefix*k*"/")
        else
            d[prefix*k] = v
        end
    end
    return d
end

# Name used in JLD2 file to identify objects stored with `save_object`
const SINGLE_OBJECT_NAME = "single_stored_object"

"""
    save_object(filename, x)

Stores an object `x` in a new JLD2 file at `filename`. If a file exists at this
path, it will be overwritten.

Since the JLD2 format requires that all objects have a name, the object will be
stored as `single_sotred_object`. If you want to store more than one object, use
[`@save`](@ref) macro, [`jldopen`](@ref) or the FileIO API.

# Example

To save the string `hello` to the JLD2 file example.jld2:

    hello = "world"
    save_object("example.jld2", hello)
"""
function save_object(filename, x)
  jldopen(filename, "w") do file
    file[SINGLE_OBJECT_NAME] = x
  end
  return
end

"""
    load_object(filename)

Returns the only available object from the JLD2 file `filename` (The stored
object name is inconsequential). If the file contains more than one or no
objects, the function throws an `ArgumentError`.

For loading more than one object, use [`@load`](@ref) macro, [`jldopen`](@ref)
or the FileIO API.

# Example

To load the only object from the JLD2 file example.jld2:

    hello = "world"
    save_object("example.jld2", hello)
    hello_loaded = load_object("example.jld2")
"""
function load_object(filename)
  jldopen(filename, "r") do file
    all_keys = keys(file)
    length(all_keys) == 0 && throw(ArgumentError("File $filename does not contain any object"))
    length(all_keys) > 1 && throw(ArgumentError("File $filename contains more than one object. Use `load` or `@load` instead"))
    file[all_keys[1]] #Uses HDF5 functionality of treating the file like a dict
  end
end


"""
    jldsave(filename, compress=false; kwargs...)

Creates a JLD2 file at `filename` and stores the variables given as keyword arguments.

# Examples

    jldsave("example.jld2"; a=1, b=2, c)
    
is equivalent to

    jldopen("example.jld2, "w") do f
        f["a"] = 1
        f["b"] = 2
        f["c"] = c
    end


To choose the io type `IOStream` instead of the default `MmapIO` use 
`jldsave{IOStream}(fn; kwargs...)`.
"""
function jldsave(filename::AbstractString, compress=false, iotype::T=MmapIO; 
                    kwargs...
                    ) where T<:Union{Type{IOStream},Type{MmapIO}}
    jldopen(filename, "w"; compress=compress, iotype=iotype) do f
        wsession = JLDWriteSession()
        for (k,v) in pairs(kwargs)
            write(f, string(k), v, wsession)
        end
    end
end

jldsave(filename::AbstractString, iotype::Union{Type{IOStream},Type{MmapIO}}; kwargs...) = 
    jldsave(filename, false, iotype; kwargs...)