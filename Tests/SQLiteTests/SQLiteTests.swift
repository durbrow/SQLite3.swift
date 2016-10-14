import XCTest
@testable import SQLite

class SQLiteTests: XCTestCase {
    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        let db = try SQLite.Database()
        try db.execute(sql: "CREATE TABLE foo ( bar INTEGER, baz TEXT )")
        try db.execute(sql: "INSERT INTO foo ( bar, baz ) VALUES ( ?, ? )", parameters: [ [ "1", "frotz" ], [ "2", "nozzl" ] ])
        try db.dump(sql: "SELECT * FROM foo")
        let stm = try db.prepare(sql: "SELECT * FROM foo")
        var i = 0
        while try stm.step() {
            let bar = Int(stm[0] ?? "")
            let baz = stm[1]
            i += 1
            switch i {
            case 1:
                XCTAssert(bar == 1)
                XCTAssert(baz == "frotz")
            case 2:
                XCTAssert(bar == 2)
                XCTAssert(baz == "nozzl")
            default:
                XCTFail()
            }
        }
    }


    static var allTests : [(String, (SQLiteTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
