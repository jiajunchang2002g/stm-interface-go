#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <poll.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/un.h>
#include <sys/socket.h>

#include <atomic>
#include <unordered_set>

#include "io.h"

static char* lineBuffer;
static size_t lineBufferSize = 0;
static std::atomic<bool> mainIsExiting {0};

static void* PollThread(void* fdPtr)
{
	struct pollfd pfd {};
	pfd.fd = (int) (long) fdPtr;
	pfd.events = 0;

	while(!mainIsExiting)
	{
		if(poll(&pfd, 1, -1) == -1)
		{
			perror("poll");
			_exit(1);
		}

		if(mainIsExiting)
			break;

		if(pfd.revents & (POLLERR | POLLHUP))
		{
			fprintf(stderr, "Connection closed by server\n");
			_exit(0);
		}
	}

	return 0;
}

int main(int argc, char* argv[])
{
	if(argc < 2)
	{
		fprintf(stderr, "Usage: %s <path of socket to connect to> < <input>\n", argv[0]);
		return 1;
	}

	int clientfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if(clientfd == -1)
	{
		perror("socket");
		return 1;
	}

	{
		struct sockaddr_un sockaddr {};
		sockaddr.sun_family = AF_UNIX;
		strncpy(sockaddr.sun_path, argv[1], sizeof(sockaddr.sun_path) - 1);
		if(connect(clientfd, (const struct sockaddr*) &sockaddr, sizeof(sockaddr)) != 0)
		{
			perror("connect");
			return 1;
		}
	}

	FILE* client = fdopen(clientfd, "r+");
	setbuf(client, NULL);

	pthread_t pollThreadHandle;
	if(pthread_create(&pollThreadHandle, NULL, PollThread, (void*) (long) clientfd) < 0)
	{
		fprintf(stderr, "Failed to create poll thread\n");
		return 1;
	}

	StmCommandType previousType = StmCommandType::COMMIT;
	uint32_t currentId = -1;
	std::unordered_set<uint32_t> idSet;

	while(true)
	{
		StmCommand input {};

		ssize_t lineLength = getline(&lineBuffer, &lineBufferSize, stdin);
		if(lineLength == -1)
			break;

		switch(lineBuffer[0])
		{
			case '#':
			case '\n':
			case '@': continue;
			case static_cast<char>(StmCommandType::START):
			{
				input.commandType = StmCommandType::START;
				if(sscanf(lineBuffer + 1, " %u", &input.transactionId) != 1)
				{
					fprintf(stderr, "Invalid begin order: %s\n", lineBuffer);
					return 1;
				}

				if (input.commandType == previousType)
				{
					fprintf(stderr, "Can not start transaction %u without committing previous transaction %u\n", input.transactionId, currentId);
					return 1;
				} else if (idSet.count(input.transactionId))
				{
					fprintf(stderr, "Transaction with id %u has already been seen", input.transactionId);
					return 1;
				}
				previousType = input.commandType;
				currentId = input.transactionId;
				idSet.insert(currentId);

				break;
			};
			case static_cast<char>(StmCommandType::COMMIT):
			{
				input.commandType = StmCommandType::COMMIT;
				if(sscanf(lineBuffer + 1, " %u", &input.transactionId) != 1)
				{
					fprintf(stderr, "Invalid commit order: %s\n", lineBuffer);
					return 1;
				}

				if (input.commandType == previousType)
				{
					fprintf(stderr, "Can not commit transaction %u without starting the transaction\n", input.transactionId);
					return 1;
				} else if (input.transactionId != currentId)
				{
					fprintf(stderr, "Commit transaction id %u does not match start transaction id %u\n", input.transactionId, currentId);
					return 1;
				}
				previousType = input.commandType;

				break;
			}
			case static_cast<char>(StmCommandType::READ):
			{
				input.commandType = StmCommandType::READ;
				if(sscanf(lineBuffer + 1, " %u", &input.address) != 1)
				{
					fprintf(stderr, "Invalid read order: %s\n", lineBuffer);
					return 1;
				}
				input.transactionId = currentId;
				break;
			}
			case static_cast<char>(StmCommandType::WRITE):
			{
				input.commandType = StmCommandType::WRITE;
				if(sscanf(lineBuffer + 1, " %u %u", &input.address, &input.value) != 2)
				{
					fprintf(stderr, "Invalid write order: %s\n", lineBuffer);
					return 1;
				}
				input.transactionId = currentId;
				break;
			}
			default: fprintf(stderr, "Invalid command '%c'\n", lineBuffer[0]); return 1;

			input.transactionId = currentId;
		}

		if(fwrite(&input, 1, sizeof(input), client) != sizeof(input))
		{
			fprintf(stderr, "Failed to send command\n");
			return 1;
		}
	}

	mainIsExiting = 1;
	fclose(client);

	return ferror(stderr) ? 1 : 0;
}
