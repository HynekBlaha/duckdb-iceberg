#include "catch.hpp"
#include "duckdb.hpp"
#include "iceberg_options.hpp"
#include "planning/scan_plan/iceberg_scan_planner.hpp"
#include "planning/scan_plan/iceberg_scan_statistics.hpp"

using namespace duckdb;

namespace {

class RejectDataManifestFileSystem : public FileSystem {
public:
	explicit RejectDataManifestFileSystem(string manifest_list) : manifest_list(std::move(manifest_list)) {
	}

	string GetName() const override {
		return "RejectDataManifestFileSystem";
	}
	bool CanHandleFile(const string &path) override {
		return StringUtil::EndsWith(path, ".avro") && !StringUtil::EndsWith(path, manifest_list);
	}
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		throw IOException("Data manifest opened while counting files");
	}

private:
	string manifest_list;
};

IcebergManifestListEntry ManifestWithCounts(optional<idx_t> added, optional<idx_t> existing) {
	IcebergManifest manifest(0, IcebergManifestContentType::DATA, 0);
	manifest.counts.emplace();
	manifest.counts->added_files_count = added;
	manifest.counts->existing_files_count = existing;
	manifest.counts->deleted_files_count = 100;
	return IcebergManifestListEntry(IcebergManifestFile("unread.avro", 0, 0, std::move(manifest)));
}

} // namespace

TEST_CASE("Iceberg counts live data files from manifest summaries", "[iceberg_native][iceberg_statistics]") {
	auto first = ManifestWithCounts(2, 3);
	auto second = ManifestWithCounts(5, 7);
	vector<BoundIcebergManifestListEntry> data {{0, first}, {1, second}};
	vector<bool> matches {true, true};
	vector<BoundIcebergManifestListEntry> deletes;
	vector<bool> delete_matches;
	IcebergScanStatistics statistics(data, matches, deletes, delete_matches);

	SECTION("Added and existing files count, deleted entries do not") {
		REQUIRE(statistics.ExactFileCount() == 17);
	}
	SECTION("Only matching manifests count") {
		matches[0] = false;
		REQUIRE(statistics.ExactFileCount() == 12);
	}
	SECTION("No matching manifests means zero files") {
		matches = {false, false};
		REQUIRE(statistics.ExactFileCount() == 0);
	}
	SECTION("Delete manifests do not remove data files from the scan") {
		deletes.emplace_back(0, first);
		delete_matches.push_back(true);
		REQUIRE(statistics.ExactFileCount() == 17);
	}
	SECTION("Missing counts in an excluded manifest are harmless") {
		first.GetManifest().counts.reset();
		matches[0] = false;
		REQUIRE(statistics.ExactFileCount() == 12);
	}
	SECTION("Missing manifest counts are unknown") {
		first.GetManifest().counts.reset();
		REQUIRE_FALSE(statistics.ExactFileCount());
	}
	SECTION("Missing added count is unknown") {
		first.GetManifest().counts->added_files_count.reset();
		REQUIRE_FALSE(statistics.ExactFileCount());
	}
	SECTION("Missing existing count is unknown") {
		first.GetManifest().counts->existing_files_count.reset();
		REQUIRE_FALSE(statistics.ExactFileCount());
	}
	SECTION("Zero is a known count") {
		first.GetManifest().counts->added_files_count = 0;
		first.GetManifest().counts->existing_files_count = 0;
		matches[1] = false;
		REQUIRE(statistics.ExactFileCount() == 0);
	}
}

TEST_CASE("An empty Iceberg manifest list has zero data files", "[iceberg_native][iceberg_statistics]") {
	vector<BoundIcebergManifestListEntry> manifests;
	vector<bool> matches;
	IcebergScanStatistics statistics(manifests, matches, manifests, matches);
	REQUIRE(statistics.ExactFileCount() == 0);
}

TEST_CASE("Iceberg file count does not open data manifests", "[iceberg_native][iceberg_statistics]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto load = connection.Query("LOAD avro");
	REQUIRE_FALSE(load->HasError());

	const string path = "data/persistent/partition_integer";
	const auto metadata_path = path + "/metadata/v2.metadata.json";
	auto &fs = FileSystem::GetFileSystem(*connection.context);
	auto metadata = IcebergTableMetadata::FromTableMetadata(IcebergTableMetadata::Parse(metadata_path, fs, "none"));
	auto snapshot = metadata.GetLatestSnapshot();
	REQUIRE(snapshot);
	auto &schema = metadata.GetLatestSchema();
	auto scan_info = make_shared_ptr<IcebergScanInfo>(metadata_path, metadata,
	                                                  IcebergSnapshotScanInfo {snapshot, schema.schema_id}, schema);
	fs.RegisterSubSystem(make_uniq<RejectDataManifestFileSystem>(snapshot->manifest_list));
	IcebergOptions options;
	IcebergScanPlanner planner(*connection.context, scan_info, path, options);
	REQUIRE(planner.GetTotalFileCount() == 2);
	REQUIRE(planner.GetTotalFileCount() == 2);
}
