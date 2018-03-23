#include "BlockIndexer.h"

void setSections(std::vector<std::vector<std::string>>&, const std::vector<std::string>&, int, int);

int main() {

	std::queue<std::string> dirs;

	for (const auto& dir : fs::directory_iterator("pa1-data")) {
		std::string path = fs::path(dir).string();
		dirs.push(path);
	}

	int maxThreads = std::thread::hardware_concurrency();
	std::vector<BlockIndexer> indexers(maxThreads);
	std::vector<std::thread> threads;

	for (auto& indexer : indexers) { // index blocks on separate threads
		threads.push_back(std::move(std::thread(
			&BlockIndexer::index,
			std::ref(indexer), 
			std::ref(dirs) )));
	}

	for (auto& thread : threads) { thread.join(); }
	
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