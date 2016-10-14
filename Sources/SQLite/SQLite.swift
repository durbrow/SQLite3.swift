import CSQLite

public enum SQLiteError : Error {
    case NoError
    case Error(code: Int, message: String)
}

private func convert_sqlite_info_array(count: Int, array: UnsafeMutablePointer<UnsafeMutablePointer<Int8>?>?) -> [String?]
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

    public func prepare(sql: String) throws -> Statement {
        var pStm : OpaquePointer?
        let rc = sqlite3_prepare_v2(dbh, sql, -1, &pStm, nil)
        if rc == SQLITE_OK {
            return Statement(pStm!)
        }
        throw SQLiteError.Error(code: Int(rc), message: errorMessage)
    }
}

private typealias sqlite3_exec_callback = @convention(c) (UnsafeMutableRawPointer?, CInt, UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?, UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> CInt

private extension Database {
    func exec(_ sql: UnsafePointer<CChar>!) -> (CInt, String?)
    {
        var errmsg : UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(dbh, sql, nil, nil, &errmsg)
        if rc == SQLITE_OK || errmsg == nil { return (rc, nil) }
        return (rc, String(cString: errmsg!))
    }
    
    func exec(
          _ sql: UnsafePointer<CChar>!
        , _ context: UnsafeMutableRawPointer
        , _ callback: @escaping sqlite3_exec_callback
        ) -> (CInt, String?)
    {
        var errmsg : UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(dbh, sql, callback, context, &errmsg)
        if rc == SQLITE_OK || errmsg == nil { return (rc, nil) }
        defer { sqlite3_free(errmsg) }
        return (rc, String(cString: errmsg!))
    }
}

public extension Database {
    public func execute(sql: String) throws
    {
        let (rc, errmsg) = exec(sql)
        if rc == SQLITE_OK { return }
        throw SQLiteError.Error(code: Int(rc), message: errmsg ?? "(no message)")
    }
    
    public func execute(sql: String, f: @escaping ([String?], [String?])->(Bool)) throws {
        struct CContext {
            let function : ([String?], [String?])->(Bool)
        }
        var context = CContext(function: f)
        let (rc, errmsg) = exec(sql, &context)
        { UP, N, coldata, colname in
            let data = convert_sqlite_info_array(count: Int(N), array: coldata)
            let name = convert_sqlite_info_array(count: Int(N), array: colname)
            let rslt = UP!.bindMemory(to: CContext.self, capacity: 1).pointee.function(data, name)
            return rslt ? 0 : 1
        }
        if rc == SQLITE_OK { return }
        throw SQLiteError.Error(code: Int(rc), message: errmsg ?? "(no message)")
    }
    
    public func execute(sql: String, parameters: () -> [String?]?) throws
    {
        let stm = try prepare(sql: sql)
        while let params = parameters() {
            try stm.bind(values: params)
            _ = try stm.step()
            try stm.reset()
        }
    }
    
    public func execute(sql: String, parameters: [[String?]]) throws
    {
        var iter = parameters.makeIterator()
        try execute(sql: sql) { iter.next() }
    }
}

public extension Database {
    public func begin() throws {
        let (rc, errmsg) = exec("BEGIN TRANSACTION")
        if rc == SQLITE_OK { return }
        throw SQLiteError.Error(code: Int(rc), message: errmsg ?? "(no message)")
    }
    public func commit() throws {
        let (rc, errmsg) = exec("COMMIT TRANSACTION")
        if rc == SQLITE_OK { return }
        throw SQLiteError.Error(code: Int(rc), message: errmsg ?? "(no message)")
    }
    public func rollback() throws {
        let (rc, errmsg) = exec("ROLLBACK TRANSACTION")
        if rc == SQLITE_OK { return }
        throw SQLiteError.Error(code: Int(rc), message: errmsg ?? "(no message)")
    }
}

public extension Database {
    public func execute(sql: String, committingEvery soOften: Int, parameters: () -> [String?]?) throws
    {
        if soOften <= 1 {
            if soOften < 1 { try! begin() }
            try execute(sql: sql, parameters: parameters)
            if soOften < 1 { try! commit() }
            return
        }
        var countdown = soOften

        try! begin()
        let stm = try prepare(sql: sql)
        while let params = parameters() {
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
    public func dump(sql: String) throws
    {
        try execute(sql: sql)
        { data, name in
            for (i, v) in data.enumerated() {
                print("\(name[i] ?? "\(i)"): \(v ?? "NULL")")
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
    guard let p = sqlite3_malloc(Int32(length + 1)) else {
        return UnsafeBufferPointer(start: nil, count: 0)
    }
#endif
    value.withCString { p.copyBytes(from: $0, count: length + 1) }
    let q = UnsafePointer(p.bindMemory(to: Int8.self, capacity: length + 1))
    return UnsafeBufferPointer(start: q, count: length)
}

private let sqlite_free : @convention(c) (UnsafeMutableRawPointer?) -> () = { sqlite3_free($0) }
private let sqlite_nofree : @convention(c) (UnsafeMutableRawPointer?) -> () = { _ in return }

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
            let rc = sqlite3_bind_text64(stm, Int32(i), value.baseAddress, sqlite3_uint64(value.count), sqlite_free, UInt8(SQLITE_UTF8))
            guard rc == SQLITE_OK else { throw SQLiteError.Error(code: Int(rc), message: errorMessage) }
        }
        else {
            let rc = sqlite3_bind_null(stm, Int32(i))
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
    
    public func bind(number i: Int, value: UnsafeBufferPointer<Void>) throws
    {
#if arch(arm64) || arch(x86_64)
        let rc = sqlite3_bind_blob64(stm, Int32(i), value.baseAddress, sqlite3_uint64(value.count), sqlite_nofree)
#else
        let rc = sqlite3_bind_blob(stm, Int32(i), value.baseAddress, Int32(value.count), sqlite_nofree)
#endif
        guard rc == SQLITE_OK else { throw SQLiteError.Error(code: Int(rc), message: errorMessage) }
    }
    
    public func bind(name: String, value: UnsafeBufferPointer<Void>) throws
    {
        let i = Int(sqlite3_bind_parameter_index(stm, name))
        if i > 0 {
            try bind(number: i, value: value)
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
        guard let data = sqlite3_column_text(stm, Int32(i)) else { return nil }
        return String(cString: data)
    }
    
    public func blob(_ i: Int) -> UnsafeBufferPointer<Void> {
        guard let data = sqlite3_column_blob(stm, Int32(i))
            else { return UnsafeBufferPointer(start: nil, count: 0) }
        let bytes = sqlite3_column_bytes(stm, Int32(i))
        return UnsafeBufferPointer(start: data.bindMemory(to: Void.self, capacity: Int(bytes)), count: Int(bytes))
    }

    public func reset() throws {
        let rc = sqlite3_reset(stm)
        guard rc == SQLITE_OK else {
            throw SQLiteError.Error(code: Int(rc), message: errorMessage)
        }
    }
}
