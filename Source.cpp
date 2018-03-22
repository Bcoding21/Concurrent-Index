#include "BlockIndexer.h"

void setSections(std::vector<std::vector<std::string>>&, const std::vector<std::string>&, int, int);

int main() {

	std::vector<std::string> directories;

	std::transform( // add sub directories to list
		fs::directory_iterator("pa1-data"),
		fs::directory_iterator(),
		std::back_inserter(directories),
		[](const auto& entry) {
		return fs::path(entry).string(); }
	);

	std::vector<std::vector<std::string>> sections; 
	int maxThreads = std::thread::hardware_concurrency();
	setSections(sections, directories, maxThreads, 0); // split directories into sections
													   // section size depends on num cores

	short directoriesLeft = directories.size();
	short j = 0;

	while (directoriesLeft) {

		short numThreads = (directoriesLeft >= maxThreads) ? maxThreads : directoriesLeft;
		directoriesLeft -= numThreads;

		std::vector<BlockIndexer> indexers(numThreads - 1);

		std::vector<std::thread> threads;
		for (int i = 0; i < numThreads - 1; i++) { // index files in directories on separate threads
			threads.push_back(
				std::move(
				std::thread(
				&BlockIndexer::index,
				std::ref(indexers[i]),
				std::ref(directories[j++])
				)));
		}

		BlockIndexer indexer; // index 1 directory in main thread
		indexer.index(directories[j++]);

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