import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2, Film, Tv } from "lucide-react";

const genres = [
    "Action", "Adventure", "Animation", "Comedy", "Crime",
    "Documentary", "Drama", "Family", "Fantasy", "History",
    "Horror", "Music", "Mystery", "Romance", "Science Fiction",
    "Thriller", "War", "Western"
];

export default function ContentRecommender() {
    const [preferences, setPreferences] = useState({
        favorite_genres: [],
        minimum_rating: 7.0,
        content_type: 'movies' // 'movies' or 'shows'
    });
    const [recommendations, setRecommendations] = useState({
        movies: [],
        shows: []
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

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
            content_type: type
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
                    <Button
                        variant={isActive ? "default" : "outline"}
                        className="gap-2"
                        onClick={() => handleContentTypeChange(contentType)}
                    >
                        {icon}
                        {title}
                    </Button>
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
                        <div className="space-y-2">
                            <Label htmlFor="genres">Favorite Genres</Label>
                            <select
                                id="genres"
                                multiple
                                className="w-full min-h-[200px] rounded-lg border border-input bg-background px-3 py-2 text-sm shadow-sm transition-colors focus:outline-none focus:ring-1 focus:ring-ring"
                                value={preferences.favorite_genres}
                                onChange={handleGenreChange}
                            >
                                {genres.map(genre => (
                                    <option key={genre} value={genre} className="py-1">
                                        {genre}
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
                        {renderContentSection('movies', 'Movies', <Film className="h-4 w-4" />)}
                        {renderContentSection('shows', 'TV Shows', <Tv className="h-4 w-4" />)}
                    </div>
                </div>
            )}
        </div>
    );
}