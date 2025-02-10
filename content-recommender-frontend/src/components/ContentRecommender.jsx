import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2, Film, Tv } from "lucide-react";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs.jsx";

const movieGenres = [
    { id: 28, name: "Action" },
    { id: 12, name: "Adventure" },
    { id: 16, name: "Animation" },
    { id: 35, name: "Comedy" },
    { id: 80, name: "Crime" },
    { id: 99, name: "Documentary" },
    { id: 18, name: "Drama" },
    { id: 10751, name: "Family" },
    { id: 14, name: "Fantasy" },
    { id: 36, name: "History" },
    { id: 27, name: "Horror" },
    { id: 10402, name: "Music" },
    { id: 9648, name: "Mystery" },
    { id: 10749, name: "Romance" },
    { id: 878, name: "Science Fiction" },
    { id: 10770, name: "TV Movie" },
    { id: 53, name: "Thriller" },
    { id: 10752, name: "War" },
    { id: 37, name: "Western" }
];

const tvGenres = [
    { id: 10759, name: "Action & Adventure" },
    { id: 16, name: "Animation" },
    { id: 35, name: "Comedy" },
    { id: 80, name: "Crime" },
    { id: 99, name: "Documentary" },
    { id: 18, name: "Drama" },
    { id: 10751, name: "Family" },
    { id: 10762, name: "Kids" },
    { id: 9648, name: "Mystery" },
    { id: 10763, name: "News" },
    { id: 10764, name: "Reality" },
    { id: 10765, name: "Sci-Fi & Fantasy" },
    { id: 10766, name: "Soap" },
    { id: 10767, name: "Talk" },
    { id: 10768, name: "War & Politics" },
    { id: 37, name: "Western" }
];

export default function ContentRecommender() {
    const [preferences, setPreferences] = useState({
        favorite_genres: [],
        minimum_rating: 7.0,
        content_type: 'movies'
    });
    const [recommendations, setRecommendations] = useState({
        movies: [],
        shows: []
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const currentGenres = preferences.content_type === 'movies' ? movieGenres : tvGenres;

    const handleGenreChange = (event) => {
        const selectedOptions = Array.from(event.target.selectedOptions, option => option.value);
        setPreferences(prev => ({
            ...prev,
            favorite_genres: selectedOptions
        }));
    };

    const handleContentTypeChange = (type) => {
        setPreferences(prev => ({
            ...prev,
            content_type: type,
            favorite_genres: [] // Reset genres when switching content type
        }));
    };

    const handleSubmit = async () => {
        if (preferences.favorite_genres.length === 0) {
            setError("Please select at least one genre");
            return;
        }

        setLoading(true);
        setError(null);

        try {
            const response = await fetch('http://localhost:8080/recommendations', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': 'true',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST,DELETE',
                },
                body: JSON.stringify(preferences),
            });

            if (!response.ok) {
                throw new Error('Failed to fetch recommendations');
            }

            const data = await response.json();
            setRecommendations(prev => ({
                ...prev,
                [preferences.content_type]: data
            }));
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const renderContentSection = (contentType, title, icon) => {
        const isActive = preferences.content_type === contentType;
        const content = recommendations[contentType];

        return (
            <div className="space-y-6">
                <div className="flex items-center gap-4">
                    {isActive && content.length > 0 && (
                        <span className="text-sm text-muted-foreground">
                            {content.length} results
                        </span>
                    )}
                </div>

                {isActive && content.length > 0 && (
                    <div className="grid gap-6 sm:grid-cols-2">
                        {content.map((item, index) => (
                            <Card key={index} className="backdrop-blur bg-card/50">
                                <CardHeader>
                                    <CardTitle className="text-lg line-clamp-1">{item.title}</CardTitle>
                                    <div className="text-sm text-muted-foreground">
                                        {item.year} • Rating: {item.rating?.toFixed(1)}
                                    </div>
                                </CardHeader>
                                <CardContent>
                                    <p className="text-sm mb-4 line-clamp-3">{item.description}</p>
                                    <div className="space-y-3">
                                        <div className="flex flex-wrap gap-2">
                                            {item.genre.map((genre, i) => (
                                                <Badge key={i} variant="outline" className="bg-accent/50">
                                                    {genre}
                                                </Badge>
                                            ))}
                                        </div>
                                        {item.where_to_watch?.length > 0 && (
                                            <div>
                                                <span className="text-sm font-medium text-muted-foreground">Watch on: </span>
                                                <div className="flex flex-wrap gap-2 mt-1">
                                                    {item.where_to_watch.map((platform, i) => (
                                                        <Badge key={i} variant="secondary">
                                                            {platform}
                                                        </Badge>
                                                    ))}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="space-y-8">
            <Card className="backdrop-blur bg-card/50">
                <CardHeader>
                    <CardTitle>Your Preferences</CardTitle>
                </CardHeader>
                <CardContent className="space-y-6">
                    <div className="space-y-4">
                        <Tabs value={preferences.content_type} onValueChange={handleContentTypeChange} className="w-full">
                            <TabsList className="grid w-full grid-cols-2">
                                <TabsTrigger value="movies" className="flex items-center gap-2">
                                    <Film className="h-4 w-4" />
                                    Movies
                                </TabsTrigger>
                                <TabsTrigger value="shows" className="flex items-center gap-2">
                                    <Tv className="h-4 w-4" />
                                    TV Shows
                                </TabsTrigger>
                            </TabsList>
                        </Tabs>

                        <div className="space-y-2">
                            <Label htmlFor="genres">Favorite Genres</Label>
                            <select
                                id="genres"
                                multiple
                                className="w-full min-h-[200px] rounded-lg border border-input bg-background px-3 py-2 text-sm shadow-sm transition-colors focus:outline-none focus:ring-1 focus:ring-ring"
                                value={preferences.favorite_genres}
                                onChange={handleGenreChange}
                            >
                                {currentGenres.map(genre => (
                                    <option key={genre.id} value={genre.name} className="py-1">
                                        {genre.name}
                                    </option>
                                ))}
                            </select>
                            <div className="flex flex-wrap gap-2 mt-2">
                                {preferences.favorite_genres.map(genre => (
                                    <Badge key={genre} variant="secondary">
                                        {genre}
                                    </Badge>
                                ))}
                            </div>
                        </div>

                        <div className="space-y-2">
                            <Label htmlFor="rating">Minimum Rating</Label>
                            <Input
                                id="rating"
                                type="number"
                                min="0"
                                max="10"
                                step="0.1"
                                value={preferences.minimum_rating}
                                onChange={(e) => setPreferences(prev => ({
                                    ...prev,
                                    minimum_rating: parseFloat(e.target.value)
                                }))}
                            />
                        </div>

                        <Button
                            onClick={handleSubmit}
                            disabled={loading}
                            className="w-full"
                        >
                            {loading ? (
                                <>
                                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                    Finding recommendations...
                                </>
                            ) : (
                                'Get Recommendations'
                            )}
                        </Button>
                    </div>

                    {error && (
                        <Alert variant="destructive">
                            <AlertDescription>{error}</AlertDescription>
                        </Alert>
                    )}
                </CardContent>
            </Card>

            {(recommendations.movies.length > 0 || recommendations.shows.length > 0) && (
                <div className="space-y-8">
                    <h3 className="text-2xl font-bold tracking-tight">Your Recommendations</h3>
                    <div className="space-y-8">
                        {renderContentSection(preferences.content_type,
                            preferences.content_type === 'movies' ? 'Movies' : 'TV Shows',
                            preferences.content_type === 'movies' ? <Film className="h-4 w-4" /> : <Tv className="h-4 w-4" />
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}