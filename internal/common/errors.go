// Copyright 2024 LatentFS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrExists        = errors.New("already exists")
	ErrNotDir        = errors.New("not a directory")
	ErrIsDir         = errors.New("is a directory")
	ErrNotEmpty      = errors.New("directory not empty")
	ErrInvalidPath   = errors.New("invalid path")
	ErrInvalidHandle = errors.New("invalid handle")
	ErrReadOnly      = errors.New("read-only filesystem")
	ErrIO            = errors.New("I/O error")
)
