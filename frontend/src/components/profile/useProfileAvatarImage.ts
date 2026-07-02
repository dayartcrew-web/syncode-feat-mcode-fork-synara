// FILE: useProfileAvatarImage.ts
// Purpose: Locally-persisted profile photo (a small, compressed data URL) for the avatar.
// When set it takes precedence over the accent color. Local-only, no I/O.
// Layer: web profile feature.

import { useCallback } from "react";
import { stringCodec } from "@t3tools/contracts";
import { useLocalStorage } from "~/hooks/useLocalStorage";

const PROFILE_AVATAR_IMAGE_STORAGE_KEY = "mcode:profile:avatarImage:v1";

// Empty string means "no photo".
export function useProfileAvatarImage() {
  const [stored, setStored] = useLocalStorage(
    PROFILE_AVATAR_IMAGE_STORAGE_KEY,
    "",
    stringCodec,
  );

  const image = stored.trim().length > 0 ? stored : null;

  const setImage = useCallback(
    (next: string | null) => {
      setStored(next ?? "");
    },
    [setStored],
  );

  return { image, setImage } as const;
}
