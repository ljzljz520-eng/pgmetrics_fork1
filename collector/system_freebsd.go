/*
 * Copyright 2026 RapidLoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collector

// collectSystem is unreachable on non-Linux platforms: callers gate the
// system domain with a platform_unsupported skip before dispatching here.
// The defensive skip is kept in case the gate is bypassed by mistake.
func (c *collector) collectSystem(o CollectConfig) (int, error) {
	c.skipPlatform(domainSystem, "", "system metrics collection is supported on Linux only")
	return 0, nil
}
