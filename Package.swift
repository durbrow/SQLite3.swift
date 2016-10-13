import PackageDescription

let package = Package(
    name: "SQLite",
    dependencies: [
        .Package(url: "https://github.com/durbrow/CSQLite.git", majorVersion: 1)
    ]
)
