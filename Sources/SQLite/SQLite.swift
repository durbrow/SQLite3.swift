import CSQLite

public enum SQLiteError : Error {
    case NoError
    case Error(code: Int, message: String)
}

/// a C void *
private typealias CVoidPointer = UnsafeMutableRawPointer?

/// a C char *
private typealias CCharPointer = UnsafeMutablePointer<CChar>?

/// a C char const *
private typealias CCharConstPointer = UnsafePointer<CChar>?

/// a C char **
private typealias CCharPointerPointer = UnsafeMutablePointer<CCharPointer>?

private func convertSQLiteStringArray(count: Int, array: CCharPointerPointer) -> [String?]
{
    guard let array = array else { return [] }
    return UnsafeBufferPointer(start: UnsafePointer(array), count: count).map { $0 == nil ? nil : String(cString: $0!) }
}

public final class Database {
    fileprivate let dbh : OpaquePointer
    
    /// Opens a database
    public init(path: String) throws {
        var pDb : OpaquePointer?
        let rc = sqlite3_open(path, &pDb)
        guard let obj = pDb else { throw SQLiteError.Error(code: Int(rc), message: "Out of memory") }
        if rc == SQLITE_OK {
            self.dbh = obj
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
        sqlite3_close_v2(dbh)
    }
    
    fileprivate var errorMessage : String {
        return String(cString: sqlite3_errmsg(dbh))
    }

    public func prepare(SQL sql: String) throws -> Statement {
        var pStm : OpaquePointer?
        let rc = sqlite3_prepare_v2(dbh, sql, -1, &pStm, nil)
        if rc == SQLITE_OK {
            return Statement(pStm!)
        }
        throw SQLiteError.Error(code: Int(rc), message: errorMessage)
    }
}

private func execCallback( _     context: CVoidPointer
                         , _ columnCount: CInt
                         , _  columnData: CCharPointerPointer
                         , _  columnName: CCharPointerPointer
                         ) -> CInt
{
    let f = context!.bindMemory(to: Database.ExecuteCallback.self, capacity: 1).pointee
    let data = convertSQLiteStringArray(count: Int(columnCount), array: columnData)
    let name = convertSQLiteStringArray(count: Int(columnCount), array: columnName)
    return f(data, name) ? 0 : ~0
}

/// execute SQL convenience functions
public extension Database {
    public func execute(SQL sql: String) throws
    {
        var errmsg : CCharPointer = nil
        let rc = sqlite3_exec(dbh, sql, nil, nil, &errmsg)
        defer { sqlite3_free(errmsg) }
        if rc == SQLITE_OK { return }
        let message = errmsg == nil ? "(no message)" : String(cString: errmsg!)
        throw SQLiteError.Error(code: Int(rc), message: message)
    }
    
    public typealias ExecuteCallback = ([String?], [String?])->Bool
    public func execute(SQL sql: String, f: @escaping ExecuteCallback) throws
    {
        // We'll pass our caller's callback as the context
        // but first we have to made a var copy because
        // the 4th argument to sqlite3_exec is not a pointer to const
        var context = f

        var errmsg : CCharPointer = nil
        let rc = sqlite3_exec(dbh, sql, execCallback, &context, &errmsg)
        defer { sqlite3_free(errmsg) }
        if rc == SQLITE_OK { return }
        let message = errmsg == nil ? "(no message)" : String(cString: errmsg!)
        throw SQLiteError.Error(code: Int(rc), message: message)
    }
    
    public func execute(SQL sql: String, parameters iter: AnyIterator<[String?]>) throws
    {
        let stm = try prepare(SQL: sql)
        while let params = iter.next() {
            try stm.bind(values: params)
            _ = try stm.step()
            try stm.reset()
        }
    }
    
    public func execute(SQL sql: String, parameters: [[String?]]) throws
    {
        try execute(SQL: sql, parameters: AnyIterator(parameters.makeIterator()))
    }
}

/// transactions
public extension Database {
    public func begin() throws {
        try execute(SQL: "BEGIN TRANSACTION")
    }
    public func commit() throws {
        try execute(SQL: "COMMIT TRANSACTION")
    }
    public func rollback() throws {
        try execute(SQL: "ROLLBACK TRANSACTION")
    }
}

public extension Database {
    public func execute(SQL sql: String, committingEvery soOften: Int, parameters: AnyIterator<[String?]>) throws
    {
        if soOften <= 1 {
            if soOften < 1 { try! begin() }
            try execute(SQL: sql, parameters: parameters)
            if soOften < 1 { try! commit() }
            return
        }
        var countdown = soOften

        try! begin()
        let stm = try prepare(SQL: sql)
        while let params = parameters.next() {
            try stm.bind(values: params)
            _ = try stm.step()
            try! stm.reset()
            countdown -= 1
            if countdown == 0 {
                try! commit()
                try! begin()
                countdown = soOften
            }
        }
        try! commit()
    }
}

public extension Database {
    public func dump(SQL sql: String) throws
    {
        try execute(SQL: sql)
        { data, name in
            for (n, v) in zip(name, data)  {
                print("\(n ?? ""): \(v ?? "NULL")")
            }
            print("")
            return true
        }
    }
}

/// create a copy of a Swift.String into memory that can be freed by sqlite3_free
private func sqlite_strdup(value: String) -> UnsafeBufferPointer<Int8>
{
    let length = value.utf8.count
#if arch(arm64) || arch(x86_64)
    guard let p = sqlite3_malloc64(sqlite3_uint64(length + 1)) else {
        return UnsafeBufferPointer(start: nil, count: 0)
    }
#else
    guard let p = sqlite3_malloc(CInt(length + 1)) else {
        return UnsafeBufferPointer(start: nil, count: 0)
    }
#endif
    value.withCString { p.copyBytes(from: $0, count: length + 1) }
    let q = UnsafePointer(p.bindMemory(to: Int8.self, capacity: length + 1))
    return UnsafeBufferPointer(start: q, count: length)
}

private let sqlite_free : @convention(c) (UnsafeMutableRawPointer?) -> () = { sqlite3_free($0) }

public final class Statement {
    private let stm : OpaquePointer
    
    fileprivate init(_ stm: OpaquePointer)
    {
        self.stm = stm
    }
    deinit
    {
        sqlite3_finalize(stm)
    }
    
    fileprivate var dbHandle : OpaquePointer
    {
        return sqlite3_db_handle(stm)
    }

    fileprivate var errorMessage : String
    {
        return String(cString: sqlite3_errmsg(dbHandle))
    }
        
    public func bind(number i: Int, value: String?) throws
    {
        if let value = value {
            let value = sqlite_strdup(value: value)
            let rc = sqlite3_bind_text64(stm, CInt(i), value.baseAddress, sqlite3_uint64(value.count), sqlite_free, UInt8(SQLITE_UTF8))
            guard rc == SQLITE_OK else { throw SQLiteError.Error(code: Int(rc), message: errorMessage) }
        }
        else {
            let rc = sqlite3_bind_null(stm, CInt(i))
            guard rc == SQLITE_OK else { throw SQLiteError.Error(code: Int(rc), message: errorMessage) }
        }
    }
    
    public func bind(name: String, value: String?) throws
    {
        let i = Int(sqlite3_bind_parameter_index(stm, name))
        if i > 0 {
            try bind(number: i, value: value)
        }
    }
    
    public func bind(number i: Int, blob: UnsafeBufferPointer<Void>) throws
    {
#if arch(arm64) || arch(x86_64)
        let rc = sqlite3_bind_blob64(stm, CInt(i), blob.baseAddress, sqlite3_uint64(blob.count), nil)
#else
        let rc = sqlite3_bind_blob(stm, CInt(i), blob.baseAddress, CInt(blob.count), nil)
#endif
        guard rc == SQLITE_OK else { throw SQLiteError.Error(code: Int(rc), message: errorMessage) }
    }
    
    public func bind(name: String, blob: UnsafeBufferPointer<Void>) throws
    {
        let i = Int(sqlite3_bind_parameter_index(stm, name))
        if i > 0 {
            try bind(number: i, blob: blob)
        }
    }
    
    public func bind(values: [String?]) throws
    {
        try clearBindings()
        for (i, v) in values.enumerated() {
            try bind(number: i + 1, value: v)
        }
    }
    
    public func bind(values: [String:String?]) throws
    {
        try clearBindings()
        for (k, v) in values {
            try bind(name: k, value: v)
        }
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
    
    public subscript(i: Int) -> String? {
        guard let data = sqlite3_column_text(stm, CInt(i)) else { return nil }
        return String(cString: data)
    }
    
    public func blob(_ i: Int) -> UnsafeBufferPointer<Void> {
        guard let data = sqlite3_column_blob(stm, CInt(i))
            else { return UnsafeBufferPointer(start: nil, count: 0) }
        let bytes = sqlite3_column_bytes(stm, CInt(i))
        return UnsafeBufferPointer(start: data.bindMemory(to: Void.self, capacity: Int(bytes)), count: Int(bytes))
    }

    public func reset() throws {
        let rc = sqlite3_reset(stm)
        guard rc == SQLITE_OK else {
            throw SQLiteError.Error(code: Int(rc), message: errorMessage)
        }
    }
}
