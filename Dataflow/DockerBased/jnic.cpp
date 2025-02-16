#include <jni.h>
#include <string>
#include <vector>
#include <iostream>

// Header file generated with: javac -h . JniDataflowExample.java
#include "JniDataflowExample.h"

// Thread-local error state
thread_local std::string lastError;

// Helper function to convert Java string to C++ string
std::string jstringToString(JNIEnv* env, jstring jStr) {
    if (!jStr) return "";
    
    const char* chars = env->GetStringUTFChars(jStr, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(jStr, chars);
    return result;
}

// Helper function to throw Java exception
void throwJavaException(JNIEnv* env, const char* className, const char* message) {
    jclass exClass = env->FindClass(className);
    if (exClass) {
        env->ThrowNew(exClass, message);
    }
    env->DeleteLocalRef(exClass);
}

// Implementation of the native method for string processing
JNIEXPORT jstring JNICALL Java_JniDataflowExample_processStringNative
  (JNIEnv* env, jclass cls, jstring input) {
    
    // Check for null input
    if (input == nullptr) {
        throwJavaException(env, "java/lang/NullPointerException", "Input string is null");
        return nullptr;
    }
    
    try {
        // Convert Java string to C++ string
        std::string inputStr = jstringToString(env, input);
        
        // Process the string (example: convert to uppercase)
        std::string result;
        for (char c : inputStr) {
            result += std::toupper(c);
        }
        
        // Convert back to Java string
        return env->NewStringUTF(result.c_str());
    } catch (const std::exception& e) {
        lastError = e.what();
        throwJavaException(env, "java/lang/RuntimeException", 
                          ("Native code exception: " + lastError).c_str());
        return nullptr;
    } catch (...) {
        lastError = "Unknown native error";
        throwJavaException(env, "java/lang/RuntimeException", 
                          ("Unexpected native error: " + lastError).c_str());
        return nullptr;
    }
}

// Implementation of the native method for integer array processing
JNIEXPORT jintArray JNICALL Java_JniDataflowExample_processIntArrayNative
  (JNIEnv* env, jclass cls, jintArray input) {
    
    // Check for null input
    if (input == nullptr) {
        throwJavaException(env, "java/lang/NullPointerException", "Input array is null");
        return nullptr;
    }
    
    try {
        // Get array length
        jsize length = env->GetArrayLength(input);
        
        // Get C array from Java array
        jint* elements = env->GetIntArrayElements(input, nullptr);
        if (!elements) {
            throwJavaException(env, "java/lang/OutOfMemoryError", "Failed to get array elements");
            return nullptr;
        }
        
        // Process the array (example: multiply each element by 2)
        std::vector<jint> result(length);
        for (jsize i = 0; i < length; i++) {
            result[i] = elements[i] * 2;
        }
        
        // Release the input array
        env->ReleaseIntArrayElements(input, elements, JNI_ABORT);
        
        // Create a new Java array for the result
        jintArray resultArray = env->NewIntArray(length);
        if (resultArray == nullptr) {
            throwJavaException(env, "java/lang/OutOfMemoryError", "Failed to create result array");
            return nullptr;
        }
        
        // Set the result array elements
        env->SetIntArrayRegion(resultArray, 0, length, result.data());
        
        return resultArray;
    } catch (const std::exception& e) {
        lastError = e.what();
        throwJavaException(env, "java/lang/RuntimeException", 
                          ("Native code exception: " + lastError).c_str());
        return nullptr;
    } catch (...) {
        lastError = "Unknown native error";
        throwJavaException(env, "java/lang/RuntimeException", 
                          ("Unexpected native error: " + lastError).c_str());
        return nullptr;
    }
}

// JNI_OnLoad is called when the library is loaded
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    std::cout << "Native library loaded successfully" << std::endl;
    return JNI_VERSION_1_8;
}
