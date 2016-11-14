#include <iostream>
#include <functional>
#include <string>
#include <thread>
#include <unistd.h>

#include <easylogging++.h>

namespace daemon_thread{
	class DaemonThreadFactory{
		
		public:
			DaemonThreadFactory(){

			}
			~DaemonThreadFactory(){

			}

			template <class Fn>
			void template_chore_function(Fn&& ct, const int sleep_time){
				LOG(INFO) << "Daemon Thread has been created. The sleep time is " << sleep_time;
				while (true){
					sleep(sleep_time);
					bool result = ct();
					if (!result){
						LOG(ERROR) << "Failure while performing chore task";
					}else{
						LOG(INFO) << "Successfully finished chore";
					}
				}

			}

			template <class Fn, class T>
			void create_daemon_thread(Fn&& ct, T&& obj, const int sleep_time){
				auto fn = std::bind(ct, obj);
				std::thread([fn, sleep_time, this](){this->template_chore_function(fn, sleep_time);}).detach();
			}

			

	};
}