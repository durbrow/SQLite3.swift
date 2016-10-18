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

/// A wrapper class for sqlite3 database handle. 
///
/// This is the base object in sqlite3
public final class Database {
    /// The database handle
    fileprivate let dbh : OpaquePointer
    
    /// Opens an sqlite3 database
    /// - Parameter path: The path to the database to open
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

    /// Prepares an SQL statement to be executed.
    /// - Parameter sql: The statement to be prepared.
    /// - Returns: A new Statement object.
    public func prepare(SQL sql: String) throws -> Statement {
        var pStm : OpaquePointer?
        let rc = sqlite3_prepare_v2(dbh, sql, -1, &pStm, nil)
        if rc == SQLITE_OK {
            return Statement(pStm!)
        }
        throw SQLiteError.Error(code: Int(rc), message: errorMessage)
    }
}

/// execute SQL convenience functions
public extension Database {
    public func execute(SQL sql: String) throws
    {
        let stm = try prepare(SQL: sql)
        while try stm.step() {}
    }
    
    public func execute(SQL sql: String, f: (_ values: [String?], _ names: [String?])->Bool) throws
    {
        let stm = try prepare(SQL: sql)
        while try stm.step() {
            let (v, n) = stm.result
            if !f(v, n) { break }
        }
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

/// Creates a copy of a Swift.String into memory that can be freed by sqlite3_free
/// - Parameter value: The string to be copied
/// - Returns: An UnsafeBufferPointer wrapping the new C string
private func sqlite_strdup(value: String) -> UnsafeBufferPointer<Int8>
{
    let length = value.utf8.count
    let s = value.withCString { withVaList([ $0 ]) { sqlite3_vmprintf("%s", $0) } }
    return UnsafeBufferPointer(start: s, count: length)
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
    
    public var result : (values: [String?], names: [String?]) {
        let n = columnCount
        let value : [String?] = (0..<n).map {
            let x = sqlite3_column_text(stm, CInt($0))
            return x == nil ? nil : String(cString: x!)
        }
        let names : [String?] = (0..<n).map {
            let x = sqlite3_column_name(stm, CInt($0))
            return x == nil ? nil : String(cString: x!)
        }
        return (value, names)
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
