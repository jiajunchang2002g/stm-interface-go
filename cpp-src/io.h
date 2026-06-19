// This file contains definitions used by the provided I/O code.
// There should be no need to modify this file.

#ifndef IO_H
#define IO_H

#include <stdint.h>

enum StmCommandType {
  START = 'S',
	COMMIT = 'C',
	READ = 'R',
	WRITE = 'W'
};

struct StmCommand {
  uint32_t transactionId;
	uint32_t address;
  uint32_t value;
  enum StmCommandType commandType;
};

#endif
