#pragma once

namespace nativefs {
	typedef struct free_block free_block;
	struct free_block {
		uint64_t offset;
		std::shared_ptr<free_block> next;
	};
}
