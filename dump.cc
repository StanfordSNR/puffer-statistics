#include <iostream>
#include <tuple>
#include <string>

#include <crypto++/sha.h>
#include <crypto++/hex.h>

using namespace std;
using namespace CryptoPP;

using session_key_t = tuple<string, uint32_t, uint32_t>;
/*                          user,   init_id,  expt_id */

template<class T>
string as_string(const T & t)
{
  return string(reinterpret_cast<const char *>(&t), sizeof(t));
}

string sha256(const string & input)
{
  SHA256 hash;
  string digest;
  StringSource s(input, true,
    new HashFilter(hash,
      new HexEncoder(
        new StringSink(digest))));
  return digest;
}

string session_id(const session_key_t & session_key)
{
  string concat = get<0>(session_key) +
                  as_string(get<1>(session_key)) +
                  as_string(get<2>(session_key));
  return sha256(concat);
}

int main(int argc, char * argv[])
{
  if (argc <= 0) {
    abort();
  }

  if (argc != 2) {
    cerr << "Usage: " << argv[0] << " influxdb_backup" << endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
