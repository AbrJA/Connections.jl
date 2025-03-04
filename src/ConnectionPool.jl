module Connections

    using DataStructures: CircularDeque

    export Pool, withconnection

    struct Connection{T}
        conn::T
        time::Float64
    end

    struct Pool{T}
        pool::CircularDeque{T}
        lock::Threads.Condition
        connect::Function
        disconnect::Function
        max_size::Int
        idle_timeout::Int
    end

    function Pool(connect::Function, disconnect::Function, max_size::Int, idle_timeout::Int)
        return Pool(CircularDeque{Connection}(max_size), Threads.Condition(), connect, disconnect, max_size, idle_timeout)
    end

    function Base.acquire(pool::Pool)
        lock(pool.lock) do
            while !isempty(pool.pool)
                conn = pop!(pool.pool)
                if (time() - conn.time) > pool.idle_timeout
                    pool.disconnect(conn.conn)
                else
                    return conn
                end
            end
            Connection(pool.connect(), time())
        end
    end

    function Base.release(conn::Connection, pool::Pool)
        begin
        lock(pool.lock)
            try
                push!(pool.pool, conn)
            catch
                pool.disconnect(conn.conn)
            finally
                unlock(pool.lock)
            end
        end
    end

    function withconnection(f::Function, pool::Pool)
        conn = Base.acquire(pool)
        try
            return f(conn.conn)
        finally
            Base.release(conn, pool)
        end
    end

end
