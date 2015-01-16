#include "org_opencb_hadoop_pore_NativePoreSupport.h"
#include "utils.h"

//------------------------------------------------------------------------------//

JNIEXPORT jstring JNICALL Java_org_opencb_hadoop_1pore_NativePoreSupport_getFastqs(JNIEnv *env, jobject this, jbyteArray array) {

	char *buffer = (*env)->GetByteArrayElements(env, array, NULL);
	int buffer_size = (*env)->GetArrayLength(env, array);

	char *fastqs = get_fastqs(buffer, buffer_size);

	jstring res = (*env)->NewStringUTF(env, fastqs);

	// free memory
	if (buffer) (*env)->ReleaseByteArrayElements(env, array, buffer, 0);
	if (fastqs) free(fastqs);

	// return
	return res;
}

//------------------------------------------------------------------------------//

JNIEXPORT jstring JNICALL Java_org_opencb_hadoop_1pore_NativePoreSupport_getInfo
(JNIEnv *env, jobject this, jbyteArray array) {

	char *buffer = (*env)->GetByteArrayElements(env, array, NULL);
	int buffer_size = (*env)->GetArrayLength(env, array);

	char *info = get_info(buffer, buffer_size);

	jstring res = (*env)->NewStringUTF(env, info);

	// free memory
	if (buffer) (*env)->ReleaseByteArrayElements(env, array, buffer, 0);
	if (info) free(info);

	// return
	return res;
}

//------------------------------------------------------------------------------//

JNIEXPORT jstring JNICALL Java_org_opencb_hadoop_1pore_NativePoreSupport_getEvents
(JNIEnv *env, jobject this, jbyteArray array, jstring source, jint startTime, jint endTime) {

	char *buffer = (*env)->GetByteArrayElements(env, array, NULL);
	int buffer_size = (*env)->GetArrayLength(env, array);
	char *src = (*env)->GetByteArrayElements(env, source, NULL);
	int start_time = startTime;
	int end_time = endTime;

	char *events = get_events(buffer, buffer_size, src, start_time, end_time);

	jstring res = (*env)->NewStringUTF(env, evnnts);

	// free memory
	if (buffer) (*env)->ReleaseByteArrayElements(env, array, buffer, 0);
	if (events) free(events);

	// return
	return res;
}


//------------------------------------------------------------------------------//
//------------------------------------------------------------------------------//


