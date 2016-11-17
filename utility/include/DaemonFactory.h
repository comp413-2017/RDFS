#include <iostream>
#include <functional>
#include <string>
#include <thread>
#include <unistd.h>

#include <easylogging++.h>

namespace daemon_thread{
	/**
	 * you need to instantiate this object
	 * by typing daemon_thread::DaemonThreadFactory var;
	 * before using the function create_daemon_thread()
	 *
	 */
	class DaemonThreadFactory{
		
		public:
			DaemonThreadFactory(){

			}
			~DaemonThreadFactory(){

			}
			/**
			 * This is a helper function that should not be called
			 * except by create_daemon_thread
			 * ct- is a function pointer that is the chore function
			 * sleep_time- is the amount of time the thread should sleep
			 */
			template <class Fn>
			void template_chore_function(Fn&& ct, const int sleep_time){
				while (true){
					sleep(sleep_time);
					bool result = ct();
					if (!result){
						LOG(ERROR) << "Failure while performing chore task";
					}
				}

			}

			/**
			 * This function will create another thread that will
			 * run asychronously
			 * ct- is a member function pointer which takes in no arguments
			 *     and returns a bool whether it was successful or not
			 * obj- is the object pointer that will call the chore function ct
			 * sleep_time- the amount of time you want the thread to sleep
			 *             before calling the chore function
			 *
			 * an example call of this function would be:
			 *         dmfac.create_daemon_thread(&ZkNnClient::TestDaemon, &zk_nn_client_obj, 10);
			 */
			template <class Fn, class T>
			void create_daemon_thread(Fn&& ct, T&& obj, const int sleep_time){
				auto fn = std::bind(ct, obj);
				std::thread([fn, sleep_time, this](){this->template_chore_function(fn, sleep_time);}).detach();
			}

			

	};
}