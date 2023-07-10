require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "byebug"

ENV["PGSLICE_ENV"] = "test"

$url = ENV["PGSLICE_URL"] || "postgres:///pgslice_test"
$conn = PG::Connection.new($url)
