import z from "zod";

export const JWTSchema = z.object({
  role: z.string(),
  userId: z.string(),
  _id: z.string(),
})
export type IJWTInfo = z.infer<typeof JWTSchema>

export const JWTInfoExtSchema = JWTSchema.extend({
  exp: z.number().optional(),
  iat: z.number().optional(),
  iss: z.string().optional(),
})
export type IJWTInfoExt = z.infer<typeof JWTInfoExtSchema>

export const JWTStorageSchema = z.object({
  code: z.string(),
  data: JWTInfoExtSchema 
  
})
export type IJWTStorage = z.infer<typeof JWTStorageSchema>