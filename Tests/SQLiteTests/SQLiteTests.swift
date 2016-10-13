import XCTest
@testable import SQLite

class SQLiteTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        let db = try! SQLite.Database()
        try! db.execute(sql: "CREATE TABLE foo ( bar INTEGER, baz TEXT )")
    }


    static var allTests : [(String, (SQLiteTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
