#include "BlockIndexer.h"

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