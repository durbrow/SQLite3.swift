import CSQLite

public enum SQLiteError : Error {
    case NoError
    case Error(code: Int, message: String)
}

public final class Database {
    fileprivate let obj : OpaquePointer
    
    /// Opens a database
    public init(path: String) throws {
        var pDb : OpaquePointer?
        let rc = sqlite3_open(path, &pDb)
        guard let obj = pDb else { throw SQLiteError.Error(code: Int(rc), message: "Out of memory") }
        if rc == SQLITE_OK {
            self.obj = obj
            return
        }
        defer { sqlite3_close(obj) }
        let message = String(cString: sqlite3_errmsg(obj))
        throw SQLiteError.Error(code: Int(rc), message: message)
    }
    
    /// Opens a temporary in-memory database
    public convenience init() throws {
        try self.init(path: ":memory:")
    }
    deinit {
        sqlite3_close_v2(obj)
    }
    
    fileprivate var errorMessage : String {
        return String(cString: sqlite3_errmsg(obj))
    }
    
    public func prepare(sql: String) throws -> Statement {
        var pStm : OpaquePointer?
        let rc = sqlite3_prepare_v2(obj, sql, -1, &pStm, nil)
        if rc == SQLITE_OK {
            return Statement(pStm!)
        }
        throw SQLiteError.Error(code: Int(rc), message: errorMessage)
    }
    
    public func execute(sql: String) throws {
        let stm = try prepare(sql: sql)
        while try stm.step() {}
    }
}

public final class Statement {
    private let stm : OpaquePointer
    
    fileprivate init(_ stm: OpaquePointer) {
        self.stm = stm
    }
    deinit {
        sqlite3_finalize(stm)
    }
    
    fileprivate var dbHandle : OpaquePointer {
        return sqlite3_db_handle(stm)
    }

    fileprivate var errorMessage : String {
        return String(cString: sqlite3_errmsg(dbHandle))
    }
    
    public func bind() {
        
    }
    
    public func clearBindings() throws {
        let rc = sqlite3_clear_bindings(stm)
        guard rc == SQLITE_OK else {
            throw SQLiteError.Error(code: Int(rc), message: errorMessage)
        }
    }
    
    public func step() throws -> Bool {
        let rc = sqlite3_step(stm)
        switch rc {
        case SQLITE_ROW:
            return true
        case SQLITE_DONE:
            return false
        default:
            throw SQLiteError.Error(code: Int(rc), message: errorMessage)
        }
    }
    
    public var columnCount : Int {
        return Int(sqlite3_column_count(stm))
    }
    
#if arch(arm64) || arch(x86_64)
    public func columnAsInt(_ i: Int) -> Int {
        return Int(sqlite3_column_int64(stm, Int32(i)))
    }
#else
    public func columnAsInt64(_ i: Int) -> Int64 {
        return Int64(sqlite3_column_int64(stm, Int32(i)))
    }

    public func columnAsInt(_ i: Int) -> Int {
        return Int(sqlite3_column_int(stm, Int32(i)))
    }
#endif
    
    public func columnAsDouble(_ i: Int) -> Double {
        return sqlite3_column_double(stm, Int32(i))
    }
    
    public func columnAsString(_ i: Int) -> String? {
        guard let data = sqlite3_column_text(stm, Int32(i)) else { return nil }
        return String(cString: data)
    }
    
    public func columnAsBlob(_ i: Int) -> (UnsafeRawPointer?, Int) {
        guard let data = sqlite3_column_blob(stm, Int32(i))
            else { return (nil, 0) }
        let bytes = sqlite3_column_bytes(stm, Int32(i))
        return (data, Int(bytes))
    }

    public func reset() throws {
        let rc = sqlite3_reset(stm)
        guard rc == SQLITE_OK else {
            throw SQLiteError.Error(code: Int(rc), message: errorMessage)
        }
    }
}
