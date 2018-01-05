// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"bytes"
	"fmt"

	fflib "github.com/pquerna/ffjson/fflib/v1"
)

// MarshalJSON marshal bytes to json - template
func (j *OffsetTable) MarshalJSON() ([]byte, error) {
	var buf fflib.Buffer
	if j == nil {
		buf.WriteString("null")
		return buf.Bytes(), nil
	}
	err := j.MarshalJSONBuf(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalJSONBuf marshal buff to json - template
func (j *OffsetTable) MarshalJSONBuf(buf fflib.EncodingBuffer) error {
	if j == nil {
		buf.WriteString("null")
		return nil
	}
	var err error
	var obj []byte
	_ = obj
	_ = err
	buf.WriteString(`{"offsets":`)
	/* Falling back. type=map[string]map[int]int64 kind=map */
	err = buf.Encode(j.Offsets)
	if err != nil {
		return err
	}
	buf.WriteByte('}')
	return nil
}

const (
	ffjtOffsetTablebase = iota
	ffjtOffsetTablenosuchkey

	ffjtOffsetTableOffsets
)

var ffjKeyOffsetTableOffsets = []byte("offsets")

// UnmarshalJSON umarshall json - template of ffjson
func (j *OffsetTable) UnmarshalJSON(input []byte) error {
	fs := fflib.NewFFLexer(input)
	return j.UnmarshalJSONFFLexer(fs, fflib.FFParse_map_start)
}

// UnmarshalJSONFFLexer fast json unmarshall - template ffjson
func (j *OffsetTable) UnmarshalJSONFFLexer(fs *fflib.FFLexer, state fflib.FFParseState) error {
	var err error
	currentKey := ffjtOffsetTablebase
	_ = currentKey
	tok := fflib.FFTok_init
	wantedTok := fflib.FFTok_init

mainparse:
	for {
		tok = fs.Scan()
		//	println(fmt.Sprintf("debug: tok: %v  state: %v", tok, state))
		if tok == fflib.FFTok_error {
			goto tokerror
		}

		switch state {

		case fflib.FFParse_map_start:
			if tok != fflib.FFTok_left_bracket {
				wantedTok = fflib.FFTok_left_bracket
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_key
			continue

		case fflib.FFParse_after_value:
			if tok == fflib.FFTok_comma {
				state = fflib.FFParse_want_key
			} else if tok == fflib.FFTok_right_bracket {
				goto done
			} else {
				wantedTok = fflib.FFTok_comma
				goto wrongtokenerror
			}

		case fflib.FFParse_want_key:
			// json {} ended. goto exit. woo.
			if tok == fflib.FFTok_right_bracket {
				goto done
			}
			if tok != fflib.FFTok_string {
				wantedTok = fflib.FFTok_string
				goto wrongtokenerror
			}

			kn := fs.Output.Bytes()
			if len(kn) <= 0 {
				// "" case. hrm.
				currentKey = ffjtOffsetTablenosuchkey
				state = fflib.FFParse_want_colon
				goto mainparse
			} else {
				switch kn[0] {

				case 'o':

					if bytes.Equal(ffjKeyOffsetTableOffsets, kn) {
						currentKey = ffjtOffsetTableOffsets
						state = fflib.FFParse_want_colon
						goto mainparse
					}

				}

				if fflib.EqualFoldRight(ffjKeyOffsetTableOffsets, kn) {
					currentKey = ffjtOffsetTableOffsets
					state = fflib.FFParse_want_colon
					goto mainparse
				}

				currentKey = ffjtOffsetTablenosuchkey
				state = fflib.FFParse_want_colon
				goto mainparse
			}

		case fflib.FFParse_want_colon:
			if tok != fflib.FFTok_colon {
				wantedTok = fflib.FFTok_colon
				goto wrongtokenerror
			}
			state = fflib.FFParse_want_value
			continue
		case fflib.FFParse_want_value:

			if tok == fflib.FFTok_left_brace || tok == fflib.FFTok_left_bracket || tok == fflib.FFTok_integer || tok == fflib.FFTok_double || tok == fflib.FFTok_string || tok == fflib.FFTok_bool || tok == fflib.FFTok_null {
				switch currentKey {

				case ffjtOffsetTableOffsets:
					goto handle_Offsets

				case ffjtOffsetTablenosuchkey:
					err = fs.SkipField(tok)
					if err != nil {
						return fs.WrapErr(err)
					}
					state = fflib.FFParse_after_value
					goto mainparse
				}
			} else {
				goto wantedvalue
			}
		}
	}

handle_Offsets:

	/* handler: j.Offsets type=map[string]map[int]int64 kind=map quoted=false*/

	{

		{
			if tok != fflib.FFTok_left_bracket && tok != fflib.FFTok_null {
				return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for ", tok))
			}
		}

		if tok == fflib.FFTok_null {
			j.Offsets = nil
		} else {

			j.Offsets = make(map[string]map[int]int64, 0)

			wantVal := true

			for {

				var k string

				var tmpJOffsets map[int]int64

				tok = fs.Scan()
				if tok == fflib.FFTok_error {
					goto tokerror
				}
				if tok == fflib.FFTok_right_bracket {
					break
				}

				if tok == fflib.FFTok_comma {
					if wantVal == true {
						// TODO(pquerna): this isn't an ideal error message, this handles
						// things like [,,,] as an array value.
						return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
					}
					continue
				} else {
					wantVal = true
				}

				/* handler: k type=string kind=string quoted=false*/

				{

					{
						if tok != fflib.FFTok_string && tok != fflib.FFTok_null {
							return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for string", tok))
						}
					}

					if tok == fflib.FFTok_null {

					} else {

						outBuf := fs.Output.Bytes()

						k = string(string(outBuf))

					}
				}

				// Expect ':' after key
				tok = fs.Scan()
				if tok != fflib.FFTok_colon {
					return fs.WrapErr(fmt.Errorf("wanted colon token, but got token: %v", tok))
				}

				tok = fs.Scan()
				/* handler: tmpJOffsets type=map[int]int64 kind=map quoted=false*/

				{

					{
						if tok != fflib.FFTok_left_bracket && tok != fflib.FFTok_null {
							return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for ", tok))
						}
					}

					if tok == fflib.FFTok_null {
						tmpJOffsets = nil
					} else {

						tmpJOffsets = make(map[int]int64, 0)

						wantVal := true

						for {

							var k int

							var tmpTmpJOffsets int64

							tok = fs.Scan()
							if tok == fflib.FFTok_error {
								goto tokerror
							}
							if tok == fflib.FFTok_right_bracket {
								break
							}

							if tok == fflib.FFTok_comma {
								if wantVal == true {
									// TODO(pquerna): this isn't an ideal error message, this handles
									// things like [,,,] as an array value.
									return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
								}
								continue
							} else {
								wantVal = true
							}

							/* handler: k type=int kind=int quoted=false*/

							{
								if tok != fflib.FFTok_integer && tok != fflib.FFTok_null {
									return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for int", tok))
								}
							}

							{

								if tok == fflib.FFTok_null {

								} else {

									tval, err := fflib.ParseInt(fs.Output.Bytes(), 10, 64)

									if err != nil {
										return fs.WrapErr(err)
									}

									k = int(tval)

								}
							}

							// Expect ':' after key
							tok = fs.Scan()
							if tok != fflib.FFTok_colon {
								return fs.WrapErr(fmt.Errorf("wanted colon token, but got token: %v", tok))
							}

							tok = fs.Scan()
							/* handler: tmpTmpJOffsets type=int64 kind=int64 quoted=false*/

							{
								if tok != fflib.FFTok_integer && tok != fflib.FFTok_null {
									return fs.WrapErr(fmt.Errorf("cannot unmarshal %s into Go value for int64", tok))
								}
							}

							{

								if tok == fflib.FFTok_null {

								} else {

									tval, err := fflib.ParseInt(fs.Output.Bytes(), 10, 64)

									if err != nil {
										return fs.WrapErr(err)
									}

									tmpTmpJOffsets = int64(tval)

								}
							}

							tmpJOffsets[k] = tmpTmpJOffsets

							wantVal = false
						}

					}
				}

				j.Offsets[k] = tmpJOffsets

				wantVal = false
			}

		}
	}

	state = fflib.FFParse_after_value
	goto mainparse

wantedvalue:
	return fs.WrapErr(fmt.Errorf("wanted value token, but got token: %v", tok))
wrongtokenerror:
	return fs.WrapErr(fmt.Errorf("ffjson: wanted token: %v, but got token: %v output=%s", wantedTok, tok, fs.Output.String()))
tokerror:
	if fs.BigError != nil {
		return fs.WrapErr(fs.BigError)
	}
	err = fs.Error.ToError()
	if err != nil {
		return fs.WrapErr(err)
	}
	panic("ffjson-generated: unreachable, please report bug.")
done:

	return nil
}
