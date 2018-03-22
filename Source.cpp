#include "BlockIndexer.h"

namespace fs = std::experimental::filesystem::v1;

void setSections(std::vector<std::vector<std::string>>&, const std::vector<std::string>&, int, int);

int main() {

	std::vector<std::string> directories;

	std::transform(
		fs::directory_iterator("pa1-data"),
		fs::directory_iterator(),
		std::back_inserter(directories),
		[](const auto& entry) {
		return fs::path(entry).string();
	});

	std::vector<std::vector<std::string>> sections;
	int maxThreads = std::thread::hardware_concurrency();
	setSections(sections, directories, maxThreads, 0);

	short directoryCount = directories.size();
	short dirIndex = 0;

	while (directoryCount) {

		short numThreads = (directoryCount >= maxThreads) ? maxThreads : directoryCount;
		directoryCount -= numThreads;

		std::vector<BlockIndexer> indexers(numThreads - 1);

		std::vector<std::thread> threads;
		for (int i = 0; i < numThreads - 1; i++) {
			threads.push_back(
				std::move(
				std::thread(
				&BlockIndexer::index,
				std::ref(indexers[i]),
				std::ref(directories[dirIndex++])
				)));
		}

		BlockIndexer indexer;
		indexer.index(directories[dirIndex++]);

		for (auto& thread : threads) { thread.join(); }
	}

	BlockIndexer::mergeIndexes();
}

void setSections(std::vector<std::vector<std::string>>& sections, const std::vector<std::string>& fileNames, int maxThreads, int i) {

	if (i == log(maxThreads) / log(2)) {
		sections.push_back(fileNames);
		return;
	}

	int midOffset = fileNames.size() >> 1;

	std::vector<std::string> leftHalf(fileNames.begin(), fileNames.begin() + midOffset);
	setSections(sections, leftHalf, maxThreads, i + 1);

	std::vector<std::string> rightHalf(fileNames.begin() + midOffset, fileNames.end());
	setSections(sections, rightHalf, maxThreads, i + 1);
}